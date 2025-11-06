"""Mappings for the Azul features API."""

import logging
from http import HTTPMethod

from azul_bedrock import models_restapi
from pydantic import TypeAdapter

from azul_client.api.base_api import BaseApiHandler

logger = logging.getLogger(__name__)

feature_type_count_adapter = TypeAdapter(dict[str, models_restapi.FeatureMulticountRet])
entities_in_features_count_adapter = TypeAdapter(dict[str, dict[str, models_restapi.ValueCountRet]])
entities_in_featurevalueparts_count_adapter = TypeAdapter(dict[str, dict[str, models_restapi.ValuePartCountRet]])


class Features(BaseApiHandler):
    """API for counting, and requesting features and their values from Azul."""

    def count_unique_values_in_feature(
        self, items: list[str], *, skip_count: bool = False, author: str = "", author_version: str = ""
    ) -> dict[str, models_restapi.FeatureMulticountRet]:
        """Count number of unique values for provided feature(s)."""
        return self._request_with_pydantic_model_response(
            url=self.cfg.azul_url + "/api/v0/features/values/counts",
            method=HTTPMethod.POST,
            response_model=feature_type_count_adapter,
            params={
                "skip_count": skip_count,
                "author": author,
                "author_version": author_version,
            },
            json={"items": items},
            get_data_only=True,
        )

    def count_unique_entities_in_features(
        self, items: list[str], *, skip_count: bool = False, author: str = "", author_version: str = ""
    ) -> dict[str, models_restapi.FeatureMulticountRet]:
        """Count number of unique entities for provided feature(s)."""
        return self._request_with_pydantic_model_response(
            url=self.cfg.azul_url + "/api/v0/features/entities/counts",
            method=HTTPMethod.POST,
            response_model=feature_type_count_adapter,
            params={
                "skip_count": skip_count,
                "author": author,
                "author_version": author_version,
            },
            json={"items": items},
            get_data_only=True,
        )

    def count_unique_entities_in_featurevalues(
        self,
        items: list[models_restapi.ValueCountItem],
        *,
        skip_count: bool = False,
        author: str = "",
        author_version: str = "",
    ) -> dict[str, dict[str, models_restapi.ValueCountRet]]:
        """Count unique entities for multiple feature values."""
        return self._request_with_pydantic_model_response(
            url=self.cfg.azul_url + "/api/v0/features/values/entities/counts",
            method=HTTPMethod.POST,
            response_model=entities_in_features_count_adapter,
            params={
                "skip_count": skip_count,
                "author": author,
                "author_version": author_version,
            },
            json={"items": [x.model_dump() for x in items]},
            get_data_only=True,
        )

    def count_unique_entities_in_featurevalueparts(
        self,
        items: list[models_restapi.ValuePartCountItem],
        *,
        skip_count: bool = False,
        author: str = "",
        author_version: str = "",
    ) -> dict[str, dict[str, models_restapi.ValuePartCountRet]]:
        """Count unique entities for multiple value parts."""
        return self._request_with_pydantic_model_response(
            url=self.cfg.azul_url + "/api/v0/features/values/parts/entities/counts",
            method=HTTPMethod.POST,
            response_model=entities_in_featurevalueparts_count_adapter,
            params={
                "skip_count": skip_count,
                "author": author,
                "author_version": author_version,
            },
            json={"items": [x.model_dump() for x in items]},
            get_data_only=True,
        )

    def get_all_feature_value_tags(self) -> models_restapi.ReadFeatureValueTags:
        """Get a list and count of all feature value tags."""
        return self._request_with_pydantic_model_response(
            url=self.cfg.azul_url + "/api/v0/features/all/tags",
            method=HTTPMethod.GET,
            response_model=models_restapi.ReadFeatureValueTags,
            get_data_only=True,
        )

    def get_feature_values_in_tag(self, tag: str) -> models_restapi.ReadFeatureTagValues:
        """Get feature values that are tagged with the provided tag."""
        return self._request_with_pydantic_model_response(
            url=self.cfg.azul_url + f"/api/v0/features/tags/{tag}",
            method=HTTPMethod.GET,
            response_model=models_restapi.ReadFeatureTagValues,
            get_data_only=True,
        )

    def create_feature_value_tag(self, tag: str, feature: str, value: str, security: str) -> bool:
        """Create a feature value tag."""
        return self._request(
            method=HTTPMethod.POST,
            url=self.cfg.azul_url + f"/api/v0/features/tags/{tag}",
            params={"feature": feature, "value": value},
            json={"security": security},
        ).json()

    def delete_feature_value_tag(self, tag: str, feature: str, value: str) -> bool:
        """Delete a feature value tag."""
        return self._request(
            method=HTTPMethod.DELETE,
            url=self.cfg.azul_url + f"/api/v0/features/tags/{tag}",
            params={"feature": feature, "value": value},
        ).json()

    def find_features(self, *, author: str = "", author_version: str = "") -> models_restapi.Features:
        """Find all features optionally selecting a specific author and author_version to search for."""
        return self._request_with_pydantic_model_response(
            url=self.cfg.azul_url + "/api/v0/features",
            method=HTTPMethod.GET,
            response_model=models_restapi.Features,
            params={"author": author, "author_version": author_version},
            get_data_only=True,
        )

    def find_values_in_feature(
        self,
        feature: str,
        *,
        term: str = "",
        sort_asc: bool = True,
        case_insensitive: bool = False,
        author: str = "",
        author_version: str = "",
        num_values: int = 500,
        after: str = "",
    ) -> models_restapi.ReadFeatureValues:
        """Find all the values associated with a feature.

        Note - After should only ever be set when you want to paginate and you get the after value from the previous
        query response.
        """
        return self._request_with_pydantic_model_response(
            url=self.cfg.azul_url + f"/api/v0/features/feature/{feature}",
            method=HTTPMethod.POST,
            response_model=models_restapi.ReadFeatureValues,
            params={
                "term": term,
                "sort_asc": sort_asc,
                "case_insensitive": case_insensitive,
                "author": author,
                "author_version": author_version,
                "num_values": num_values,
            },
            json={"after": after},
            get_data_only=True,
        )
