"""Module for interacting with binary endpoints."""

import logging
from dataclasses import dataclass
from http import HTTPMethod
from typing import Callable, Generator

import httpx
import pendulum
from azul_bedrock import models_network as azm
from azul_bedrock import models_restapi
from pydantic import TypeAdapter

from azul_client import config
from azul_client.api.base_api import BaseApiHandler

logger = logging.getLogger(__name__)
autocomplete_type_adapter = TypeAdapter(models_restapi.AutocompleteContext)


@dataclass
class FindOptions:
    """Store all of the options available for creating a term query.

    Note list values assume an OR relationship when looking for matches.
    Note list values assume an AND relationship when excluding matches.

    Note dict values always assume an AND relationship between members of dict.

    Note when specifying string values you can provide a wildcard '*' character.
    Typically only useful at the end, rather than the start or middle e.g when looking for al file type image/bmp:
    works:
    file_format:"image*"
    doesn't work:
    file_format:"ima*e"
    file_format:"*bmp"
    """

    _query: str = ""

    # ------------------------- Source options
    # Name(id) of the source(s) to look for entities within.
    sources: list[str] | str | None = None
    # Name(id) of the source(s) to exclude when looking for entities.
    source_excludes: list[str] | str | None = None

    # Includes only entities that have the depths provided.
    source_depth: list[int] | int | None = None
    # Exclude entities that have the depths provided.
    source_depth_exclude: list[int] | int | None = None
    # Includes only entities that have the depth greater than the value provided.
    source_depth_greater: int | None = None
    # Includes only entities that have the depth less than the value provided.
    source_depth_less: int | None = None

    # Includes only entities that have been sourced by the username.
    source_username: str | None = None
    # Includes only entities that have ALL the provided reference fields.
    source_reference: dict[str, str] | None = None  # Key value pairs of reference fields.

    # ------------------------- Time options
    # Include only entities that are newer than or equal to the provided timestamp (to nearest second)
    source_timestamp_newer_or_equal: pendulum.DateTime | None = None
    # Include only entities that are older than or equal to the provided timestamp (to nearest second)
    source_timestamp_older_or_equal: pendulum.DateTime | None = None
    # Include only entities that are newer than the provided timestamp (to nearest second)
    source_timestamp_newer: pendulum.DateTime | None = None
    # Include only entities that are older than the provided timestamp (to nearest second)
    source_timestamp_older: pendulum.DateTime | None = None

    # ------------------------- Plugin/Author filtering
    # Include entities that have valid results from the provided plugin (case sensitive).
    plugin_name: str | None = None
    # Include entities that have valid results from the provided plugin version (case sensitive).
    plugin_version: str | None = None

    # ------------------------- Feature filtering
    # Include entities that have any of the feature keys.
    has_feature_keys: list[str] | str | None = None
    # Include entities that have any of the feature values.
    has_feature_values: list[str] | str | None = None

    # ------------------------- Binary Info
    # Include entities that are greater (in bytes) than the provided value.
    greater_than_size_bytes: int | None = None
    # Include entities that are less than (in bytes) than the provided value.
    less_than_size_bytes: int | None = None

    # Include entities that have the specified file type.
    file_formats_legacy: str | list[str] | None = None
    # Exclude entities that have the specified file type.
    file_formats_legacy_exclude: str | list[str] | None = None
    # Include entities that have the specified file type (AL type).
    file_formats: str | list[str] | None = None
    # Exclude entities that have the specified file type (AL type).
    file_formats_exclude: str | list[str] | None = None

    # ------------------------- Tags
    # Find all binaries that have any of the provided tags.
    binary_tags: str | list[str] | None = None
    # Final all binaries that have features with the provided tags.
    feature_tags: str | list[str] | None = None

    # --- End of options ---

    def _add(self, value: str):
        """Append a new value to the query."""
        if not self._query:
            self._query = value
        else:
            self._query += " " + value

    def _add_date(self, search_key: str, value: pendulum.DateTime | None):
        """Add a date as an integer to the query."""
        if value is None:
            return
        # Timestamp is just milliseconds since epoch, Format docs - https://pendulum.eustace.io/docs/#string-formatting
        self._add(search_key % (value.format("x")))

    def _add_if_not_none(self, search_key: str, value: str | int | None):
        """Add a value to the internal query if the provided value isn't none."""
        if value is None:
            return
        self._add(search_key % (value))

    def _add_list(self, search_key: str, value: str | list[str] | int | list[int] | None, negation: bool = False):
        """Add a list of values to the internal query if the provided value isn't none.

        Negation is used to switch between 'AND'ing and 'OR'ing members of the list.
        """
        if value is None or (isinstance(value, list) and len(value) == 0):
            return

        # Convert to a list if required.
        if isinstance(value, str) or isinstance(value, int):
            value = [value]

        for i, val in enumerate(value):
            if i > 0:
                prefix = "OR "
                if negation:
                    prefix = "AND "
                self._add(prefix + search_key % (val))
            else:
                self._add(search_key % (val))

    def _add_key_value(self, search_key: str, value: dict[str, str] | None):
        """Add a number key value pair queries."""
        if value is None or len(value) == 0:
            return

        for k, v in value.items():
            self._add(search_key % (k, v))

    def to_query(self) -> str:
        """Convert the find options into a term query."""
        self._query = ""
        # Source options
        self._add_list('source.name:"%s"', self.sources)
        self._add_list('!source.name:"%s"', self.source_excludes, negation=True)

        self._add_list("depth:%s", self.source_depth)
        self._add_list("!depth:%s", self.source_depth_exclude, negation=True)
        self._add_if_not_none("depth:>%s", self.source_depth_greater)
        self._add_if_not_none("depth:<%s", self.source_depth_less)

        self._add_if_not_none('source.encoded_references.key_value:"user.%s"', self.source_username)
        self._add_key_value('source.encoded_references.key_value:"%s.%s"', self.source_reference)

        # Time options
        self._add_date("source.timestamp:>=%s", self.source_timestamp_newer_or_equal)
        self._add_date("source.timestamp:<=%s", self.source_timestamp_older_or_equal)
        self._add_date("source.timestamp:>%s", self.source_timestamp_newer)
        self._add_date("source.timestamp:<%s", self.source_timestamp_older)

        # Author options
        self._add_if_not_none('author.name:"%s"', self.plugin_name)
        self._add_if_not_none('author.version:"%s"', self.plugin_version)

        # Features
        self._add_list('features.name:"%s"', self.has_feature_keys)
        self._add_list('features.value:"%s"', self.has_feature_values)

        # Binary Info
        self._add_if_not_none("size:>%s", self.greater_than_size_bytes)
        self._add_if_not_none("size:<%s", self.less_than_size_bytes)

        self._add_list('file_format_legacy:"%s"', self.file_formats_legacy)
        self._add_list('!file_format_legacy:"%s"', self.file_formats_legacy_exclude, negation=True)

        self._add_list('file_format:"%s"', self.file_formats)
        self._add_list('!file_format:"%s"', self.file_formats_exclude, negation=True)

        # Tags
        self._add_list('binary.tag:"%s"', self.binary_tags)
        self._add_list('feature.tag:"%s"', self.feature_tags)

        return self._query


class BinariesMeta(BaseApiHandler):
    """Interact with binary endpoints."""

    SHA256_regex = r"^[a-fA-F0-9]{64}$"
    upload_download_timeout = 120

    def __init__(self, cfg: config.Config, get_client: Callable[[], httpx.Client]) -> None:
        """Init."""
        super().__init__(cfg, get_client)

    def check_meta(self, sha256: str) -> bool:
        """Check metadata exists for hash."""
        return self._generic_head_request(self.cfg.azul_url + f"/api/v0/binaries/{sha256}")

    def get_meta(
        self,
        sha256: str,
        *,
        details: list[models_restapi.BinaryMetadataDetail] | None = None,
        author: str | None = None,
        bucket_size: int = 100,
    ) -> models_restapi.BinaryMetadata:
        """Get metadata for hash.

        :param str sha256: sha256 of the binary to get metadata for.
        :param bool detail: Set to True to get detailed information about the binary including
        children, features, streams, parents etc.
        :param int bucket_size: Edit bucket size to get data if a query overflows the current bucket count
        (Buckets this affects are Features, Info, Streams(data) and Instances(Authors)).
        """
        params = {"detail": details, "author": author, "bucket_size": bucket_size}
        for k in list(params.keys()):
            if params[k] is None:
                params.pop(k)
        return self._request_with_pydantic_model_response(
            method=HTTPMethod.GET,
            url=self.cfg.azul_url + f"/api/v0/binaries/{sha256}",
            response_model=models_restapi.BinaryMetadata,
            params=params,
            get_data_only=True,
        )

    def _base_find(
        self,
        *,
        term: str | None = None,
        max_entities: int | None = None,
        count_entities: bool | None = None,
        hashes: list[str] | None = None,
        sort_prop: models_restapi.FindBinariesSortEnum | None = None,
        sort_asc: bool | None = None,
    ) -> models_restapi.EntityFind:
        params = dict(
            term=term,
            max_entities=max_entities,
            count_entities=count_entities,
            sort=sort_prop,
            sort_asc=sort_asc,
        )
        params = self.filter_none_values(params)

        if not hashes:
            hashes = []
        return self._request_with_pydantic_model_response(
            method=HTTPMethod.POST,
            url=self.cfg.azul_url + "/api/v0/binaries",
            params=params,
            json={"hashes": hashes},
            response_model=models_restapi.EntityFind,
            get_data_only=True,
        )

    def find(
        self,
        term: str | None,
        *,
        max_entities: int | None = None,
        count_entities: bool | None = None,
        sort_prop: models_restapi.FindBinariesSortEnum | None = None,
        sort_asc: bool | None = None,
    ) -> models_restapi.EntityFind:
        """Find binaries matching the term query, limiting to the max_entities."""
        return self._base_find(
            term=term, max_entities=max_entities, count_entities=count_entities, sort_prop=sort_prop, sort_asc=sort_asc
        )

    def find_simple(
        self,
        find_options: FindOptions,
        *,
        max_entities: int | None = None,
        count_entities: bool | None = None,
        sort_prop: models_restapi.FindBinariesSortEnum | None = None,
        sort_asc: bool | None = None,
    ) -> models_restapi.EntityFind:
        """Find binaries matching the term query, limiting to the max_entities."""
        return self._base_find(
            term=find_options.to_query(),
            max_entities=max_entities,
            count_entities=count_entities,
            sort_prop=sort_prop,
            sort_asc=sort_asc,
        )

    def find_hashes(self, hashes: list[str]) -> models_restapi.EntityFind:
        """Check if a list of hashes are in Azul and returns their basic summary information."""
        return self._base_find(hashes=hashes)

    @dataclass
    class FindAll:
        """Result of a find_all query, with iterator to look at each binary."""

        approx_total: int = 0
        iter: Generator[models_restapi.EntityFindSimpleItem, None, None] = None

        def __iter__(self):
            """Iterator over the default iterator of iter."""
            return self.iter

    def find_all(
        self,
        find_options: FindOptions,
        *,
        max_binaries: int = 0,
        request_binaries: int = 5000,
    ) -> FindAll:
        """Find all binaries matching the term query, returned via a generator."""
        if max_binaries and max_binaries < request_binaries:
            request_binaries = max_binaries

        params = dict(
            term=find_options.to_query(),
            numodels_restapi=request_binaries,
        )
        params = self.filter_none_values(params)
        resp: models_restapi.EntityFindSimple = self._request_with_pydantic_model_response(
            method=HTTPMethod.POST,
            url=self.cfg.azul_url + "/api/v0/binaries/all",
            params=params,
            json={"after": None},
            response_model=models_restapi.EntityFindSimple,
            get_data_only=True,
        )

        def _iterate_binaries(params: dict, resp: models_restapi.EntityFindSimple):
            """Iterate over the found binaries."""
            after = resp.after
            found = 0
            while True:
                after = resp.after
                if len(resp.items) == 0 or not after:
                    return
                for item in resp.items:
                    yield item
                    found += 1
                    if max_binaries and found >= max_binaries:
                        # quit even if we have more than requested that we can supply
                        return
                resp: models_restapi.EntityFindSimple = self._request_with_pydantic_model_response(
                    method=HTTPMethod.POST,
                    url=self.cfg.azul_url + "/api/v0/binaries/all",
                    params=params,
                    json={"after": after},
                    response_model=models_restapi.EntityFindSimple,
                    get_data_only=True,
                )

        return BinariesMeta.FindAll(
            approx_total=resp.total if resp.total else 0,
            iter=_iterate_binaries(params, resp) if len(resp.items) > 0 else iter([]),
        )

    def get_model(self) -> models_restapi.EntityModel:
        """Return the model for the binaries."""
        return self._request_with_pydantic_model_response(
            method=HTTPMethod.GET,
            url=self.cfg.azul_url + "/api/v0/binaries/model",
            response_model=models_restapi.EntityModel,
            get_data_only=True,
        )

    def find_autocomplete(self, term: str) -> models_restapi.AutocompleteContext:
        """Looks for potential auto-completes for a search term and returns them.

        :param str term: Term to try and auto-complete.
        """
        # NOTE - offset is auto calculated as it's used for cursor position which makes no sense
        # from a client API perspective.

        return self._request_with_pydantic_model_response(
            method=HTTPMethod.GET,
            url=self.cfg.azul_url + "/api/v0/binaries/autocomplete",
            response_model=autocomplete_type_adapter,
            params={"term": term, "offset": len(term) - 1},
            get_data_only=True,
        )

    def get_has_newer_metadata(self, sha256: str, timestamp: str) -> models_restapi.BinaryDocuments:
        """Check if a binary has data newer than the provided timestamp in ISO8601 format."""
        return self._request_with_pydantic_model_response(
            method=HTTPMethod.GET,
            url=self.cfg.azul_url + f"/api/v0/binaries/{sha256}/new",
            response_model=models_restapi.BinaryDocuments,
            params={"timestamp": timestamp},
            get_data_only=True,
        )

    def get_similar_ssdeep_entities(self, ssdeep: str, *, max_matches: int = 20) -> models_restapi.SimilarFuzzyMatch:
        """Return id and similarity score of entities with a similar ssdeep fuzzyhash."""
        if not ssdeep:
            raise ValueError("ssdeep must be set to something.")
        return self._request_with_pydantic_model_response(
            method=HTTPMethod.GET,
            url=self.cfg.azul_url + "/api/v0/binaries/similar/ssdeep",
            response_model=models_restapi.SimilarFuzzyMatch,
            params={"ssdeep": ssdeep, "max_matches": max_matches},
            get_data_only=True,
        )

    def get_similar_tlsh_entities(self, tlsh: str, *, max_matches: int = 20) -> models_restapi.SimilarFuzzyMatch:
        """Return id and similarity score of entities with a similar tlsh fuzzyhash."""
        if not tlsh:
            raise ValueError("tlsh must be set to something.")
        return self._request_with_pydantic_model_response(
            method=HTTPMethod.GET,
            url=self.cfg.azul_url + "/api/v0/binaries/similar/tlsh",
            response_model=models_restapi.SimilarFuzzyMatch,
            params={"tlsh": tlsh, "max_matches": max_matches},
            get_data_only=True,
        )

    def get_similar_entities(self, sha256: str, *, recalculate: bool = False) -> models_restapi.SimilarMatch:
        """Return information about similar entities."""
        return self._request_with_pydantic_model_response(
            method=HTTPMethod.GET,
            url=self.cfg.azul_url + f"/api/v0/binaries/{sha256}/similar",
            response_model=models_restapi.SimilarMatch,
            params={"recalculate": recalculate},
            get_data_only=True,
        )

    def get_nearby_entities(
        self,
        sha256: str,
        *,
        include_cousins: models_restapi.IncludeCousinsEnum = models_restapi.IncludeCousinsEnum.Standard,
    ) -> models_restapi.ReadNearby:
        """Get information about nearby entities (used to build relational tree graph)."""
        return self._request_with_pydantic_model_response(
            method=HTTPMethod.GET,
            url=self.cfg.azul_url + f"/api/v0/binaries/{sha256}/nearby",
            response_model=models_restapi.ReadNearby,
            params={"include_cousins": include_cousins.value},
            get_data_only=True,
        )

    def get_binary_tags(self, sha256: str) -> models_restapi.ReadAllEntityTags:
        """Return all tags for an binary."""
        return self._request_with_pydantic_model_response(
            method=HTTPMethod.GET,
            url=self.cfg.azul_url + f"/api/v0/binaries/{sha256}/tags",
            response_model=models_restapi.ReadAllEntityTags,
            get_data_only=True,
        )

    def create_tag_on_binary(self, sha256: str, tag: str, security: str) -> None:
        """Attach a tag to the provided binaries sha256."""
        return self._request(
            method=HTTPMethod.POST,
            url=self.cfg.azul_url + f"/api/v0/binaries/{sha256}/tags/{tag}",
            json={"security": security},
        )

    def delete_tag_on_binary(self, sha256: str, tag: str) -> models_restapi.AnnotationUpdated:
        """Delete the specified tag from the specified binary."""
        return self._request_with_pydantic_model_response(
            method=HTTPMethod.DELETE,
            url=self.cfg.azul_url + f"/api/v0/binaries/{sha256}/tags/{tag}",
            response_model=models_restapi.AnnotationUpdated,
            get_data_only=True,
        )

    def get_binary_status(self, sha256: str) -> models_restapi.Status:
        """Get the plugin statuses for a binary."""
        return self._request_with_pydantic_model_response(
            method=HTTPMethod.GET,
            url=self.cfg.azul_url + f"/api/v0/binaries/{sha256}/statuses",
            response_model=models_restapi.Status,
            get_data_only=True,
        )

    def get_binary_documents(
        self, sha256: str, *, action: azm.BinaryAction | None = None, size: int = 1000
    ) -> models_restapi.OpensearchDocuments:
        """Get opensearch documents for a binary.

        :param str sha256: The sha256 of the binary to look for documents for.
        :param BinaryAction action: The action to get events for.
        :param int size: Maximum number of events that will be returned.
        """
        params = {"event_type": action, "size": size}
        params = self.filter_none_values(params)

        return self._request_with_pydantic_model_response(
            method=HTTPMethod.GET,
            url=self.cfg.azul_url + f"/api/v0/binaries/{sha256}/events",
            response_model=models_restapi.OpensearchDocuments,
            params=params,
            get_data_only=True,
        )
