"""Module for interacting with binary endpoints."""

import copy
import json
import logging
import os
import re
import struct
from dataclasses import dataclass
from http import HTTPMethod
from io import BytesIO
from pathlib import Path
from tempfile import SpooledTemporaryFile
from typing import IO

import cart
import httpx
import malpz
import pendulum
import tenacity
from azul_bedrock import models_network as azm
from azul_bedrock import models_restapi

from azul_client import exceptions
from azul_client.api.base_api import BaseApiHandler

logger = logging.getLogger(__name__)
DEFAULT_MAX_BYTES_TO_READ = 10 * 1024 * 1024  # 10MB worth of strings.


class _OpenFile:
    """A handler for a potential filepath, raw bytes or a file handle."""

    handle: IO[bytes] | None

    def __init__(self, file_path_or_contents: IO[bytes] | bytes | str | Path):
        self.opened_file = False
        self.file_path_or_contents = file_path_or_contents
        self.handle = None

    def _get_file_handle(self, file_path_or_contents: Path | str | IO[bytes] | bytes):
        if isinstance(file_path_or_contents, bytes):
            self.handle = BytesIO(file_path_or_contents)
        elif isinstance(file_path_or_contents, Path):
            if not file_path_or_contents.exists():
                raise FileExistsError(f"The file with the path {file_path_or_contents=} does not exist.")
            self.opened_file = True
            self.handle = file_path_or_contents.open(mode="rb")
        elif isinstance(file_path_or_contents, str):
            if not os.path.exists(file_path_or_contents):
                raise FileExistsError(f"The file with the path {file_path_or_contents=} does not exist.")
            self.handle = open(file_path_or_contents, mode="rb")
        elif isinstance(file_path_or_contents, SpooledTemporaryFile):
            self.handle = file_path_or_contents
        else:
            # Will already be IO[bytes]
            self.handle = file_path_or_contents

    def open(self) -> IO[bytes]:
        """Open or provide the handle to a file."""
        self._get_file_handle(self.file_path_or_contents)
        return self.handle

    def close(self):
        """If a file was opened close it."""
        if self.opened_file:
            try:
                if self.handle.closed:
                    self.handle.close()
            except Exception:
                print("Failed to close a file.")
                raise

    def __enter__(self):
        """Open or provide the handle to a file."""
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """If a file was opened close it."""
        self.close()


@dataclass
class AugmentedStream:
    """An augmented way of viewing the original binary.

    E.g: the binary is compiled C# code and an augmented stream is a decompiled version of the code.
    E.g2: the original file is a jpg and the augmented stream is a png which has had any potential malware removed.
    """

    label: azm.DataLabel
    file_name: str
    contents_file_path: IO[bytes] | bytes | str | Path | _OpenFile


class _OpenAugmentedStreams:
    opened_streams: list[AugmentedStream]

    def __init__(self, streams: list[AugmentedStream]):
        self.opened_file = False
        self._in_streams = streams
        self.opened_streams = []
        self.file_handles = []
        self.contents = None

    def __enter__(self):
        for stream in self._in_streams:
            # Open the file or contents
            open_file = _OpenFile(stream.contents_file_path)
            self.file_handles.append(open_file)
            # Make a new augmented stream object with the file or contents.
            stream_copy = copy.copy(stream)
            stream_copy.contents_file_path = open_file
            self.opened_streams.append(stream_copy)
        return self.opened_streams

    def __exit__(self, exc_type, exc_val, exc_tb):
        for fh in self.file_handles:
            try:
                fh.close()
            except Exception:
                print("Warning: Failed to close one of the Augmented streams.")


class BinariesData(BaseApiHandler):
    """Interact with binary endpoints."""

    SHA256_regex = r"^[a-fA-F0-9]{64}$"
    upload_download_timeout = 120

    def check_data(self, sha256: str) -> bool:
        """Check data exists for hash."""
        return self._generic_head_request(self.cfg.azul_url + f"/api/v0/binaries/{sha256}/content")

    def download(self, sha256: str) -> bytes:
        """Download binary with the given sha256 in cart format."""
        return self._request(
            method=HTTPMethod.GET,
            url=self.cfg.azul_url + f"/api/v0/binaries/{sha256}/content",
            timeout=self.upload_download_timeout,
        ).content

    def download_bulk(self, hashes: list[str]) -> bytes:
        """Download multiple binaries with the given list of sha256 hashes."""
        return self._request(
            method=HTTPMethod.POST,
            url=self.cfg.azul_url + "/api/v0/binaries/content/bulk",
            json={"binaries": hashes},
            timeout=self.upload_download_timeout,
        ).content

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_random(min=1, max=2),
        retry=tenacity.retry_if_exception_type(httpx.TimeoutException),
        before_sleep=tenacity.before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _base_upload(
        self,
        body: dict,
        *,
        api: str,
        file_path_or_contents: Path | str | bytes | IO[bytes] | SpooledTemporaryFile | None = None,
        augmented_streams: list[AugmentedStream] | None = None,
        filename: str | None = None,
        password: str = "",
        extract: bool = False,
        refresh: bool = False,
    ) -> models_restapi.BinaryData:
        if not augmented_streams:
            augmented_streams = []

        for k, v in list(body.items()):
            if v is None:
                body.pop(k)

        with _OpenFile(file_path_or_contents) as file_handle:
            safe_file = None
            if file_handle is not None:
                # Try to identify this as either a .malpz or .cart
                is_malpz = False
                malpz_header_length = len(malpz.MALPZ_HEADER)
                malpz_header = file_handle.read(malpz_header_length)
                if len(malpz_header) == malpz_header_length:
                    # File too small otherwise
                    try:
                        malpz.validate_version(malpz_header)
                        is_malpz = True
                    except malpz.MetadataException:
                        pass
                file_handle.seek(0)

                cart_header_length = struct.calcsize(cart.MANDATORY_HEADER_FMT)
                cart_header = file_handle.read(cart_header_length)
                is_cart = cart.is_cart(cart_header)
                file_handle.seek(0)

                if not extract and not is_malpz and not is_cart:
                    # This file is not neutered; do this now before sending over the network
                    safe_file = SpooledTemporaryFile(max_size=1000 * 1000)
                    try:
                        print("Packing file as a .CaRT...")
                        cart.pack_stream(file_handle, safe_file)
                        print("CaRT size:", safe_file.tell())
                        safe_file.seek(0)
                    except BaseException:
                        # Avoid leaving temp files in case CaRTing fails. httpx should
                        # handle our file handle otherwise.
                        safe_file.close()
                        raise
                else:
                    # Use the original data source
                    safe_file = file_handle

            with _OpenAugmentedStreams(augmented_streams) as opened_streams:
                stream_data = [
                    ("stream_data", (s.file_name, s.contents_file_path.open(), "application/octet-stream"))
                    for s in opened_streams
                ]
                body["stream_labels"] = [s.label for s in opened_streams]

                main_file = []
                # If there is any contents.
                if safe_file:
                    main_file.append(("binary", (filename, safe_file, "application/octet-stream")))
                resp = self._request_upload(
                    url=self.cfg.azul_url + api,
                    params={"refresh": refresh, "extract": extract, "password": password},
                    files=main_file + stream_data,
                    data=body,
                    timeout=self.upload_download_timeout,
                )

        if resp.status_code != 200 and resp.status_code != 206:
            raise exceptions.bad_response(resp)

        entity = resp.json()[0]
        if not entity.get("id"):
            entity["id"] = entity.get("sha256")
        return models_restapi.BinaryData.model_validate(entity)

    def upload(
        self,
        file_path_or_contents: Path | str | bytes | IO[bytes] | SpooledTemporaryFile,
        source_id: str,
        *,
        references: dict[str, str] | None = None,
        submit_settings: dict[str, str] | None = None,
        augmented_streams: list[AugmentedStream] | None = None,
        filename: str | None = None,
        timestamp: str | None = None,
        security: str,
        extract: bool = False,
        password: str | None = None,
        refresh: bool = False,
        exclude_security_labels: list[str] = None,
        include_queries: bool = False,
    ) -> models_restapi.BinaryData:
        """Upload binary handle with corresponding form data."""
        # If there are no augmented stream and the file isn't being extracted their must be a filename.
        if not augmented_streams and not extract and not filename:
            raise ValueError("If the upload isn't an archive and you aren't uploading streams a filename is required.")

        if not source_id:
            raise ValueError(f"{source_id=} is required to be a valid value.")

        if security is not None and not isinstance(security, str):
            raise ValueError("Security must be a string value.")

        if not timestamp:
            timestamp = pendulum.now(pendulum.UTC).to_iso8601_string()

        references = json.dumps(references) if references else None
        submit_settings = json.dumps(submit_settings) if submit_settings else None

        return self._base_upload(
            body=dict(
                source_id=source_id,
                references=references,
                settings=submit_settings,
                timestamp=timestamp,
                security=security,
                exclude_security_labels=exclude_security_labels,
                include_queries=include_queries,
                filename=filename,
            ),
            api="/api/v0/binaries/source",
            file_path_or_contents=file_path_or_contents,
            augmented_streams=augmented_streams,
            filename=filename,
            extract=extract,
            password=password,
            refresh=refresh,
        )

    def upload_dataless(
        self,
        binary_id: str,  # sha256 of the binary to have it's metadata updated
        source_id: str,
        *,
        references: dict[str, str] | None = None,
        augmented_streams: list[AugmentedStream] | None = None,
        filename: str | None = None,
        timestamp: str | None = None,
        security: str,
        refresh: bool = False,
        exclude_security_labels: list[str] = None,
        include_queries: bool = False,
    ) -> models_restapi.BinaryData:
        """Upload new metadata and potentially alt-streams for a binary."""
        if not binary_id and re.search(self.SHA256_regex, binary_id):
            raise ValueError(f"{binary_id=} must be set to a valid sha256 value.")

        if security is not None and not isinstance(security, str):
            raise ValueError("Security must be a string value.")

        if not timestamp:
            timestamp = pendulum.now(pendulum.UTC).to_iso8601_string()

        references = json.dumps(references) if references else None

        return self._base_upload(
            body=dict(
                sha256=binary_id,
                source_id=source_id,
                references=references,
                timestamp=timestamp,
                security=security,
                exclude_security_labels=exclude_security_labels,
                include_queries=include_queries,
                filename=filename,
            ),
            api="/api/v0/binaries/source/dataless",
            augmented_streams=augmented_streams,
            filename=filename,
            refresh=refresh,
        )

    def upload_child(
        self,
        file_path_or_contents: Path | str | bytes | IO[bytes],
        parent_sha256: str,
        relationship: dict[str, str],
        *,
        submit_settings: dict[str, str] | None = None,
        parent_type: str = "binary",
        filename: str | None = None,
        timestamp: str | None = None,
        security: str,
        extract: bool = False,
        password: str | None = None,
        refresh: bool = False,
        exclude_security_labels: list[str] = None,
        include_queries: bool = False,
    ) -> models_restapi.BinaryData:
        """Upload a child binary and attach it to the parent binary with the provided sha256 ID."""
        if not parent_sha256 or not re.search(self.SHA256_regex, parent_sha256):
            raise ValueError(f"{parent_sha256=} must be set to a valid sha256 value.")

        if not relationship:
            raise ValueError(f"{relationship=} must be a dictionary with at least one key value pair.")
        relationship = json.dumps(relationship) if relationship else None
        submit_settings = json.dumps(submit_settings) if submit_settings else None

        if not extract and not filename:
            raise ValueError("If the upload isn't an archive a filename is required.")

        if security is not None and not isinstance(security, str):
            raise ValueError("Security must be a string value.")

        if not timestamp:
            timestamp = pendulum.now(pendulum.UTC).to_iso8601_string()

        return self._base_upload(
            body=dict(
                timestamp=timestamp,
                security=security,
                exclude_security_labels=exclude_security_labels,
                include_queries=include_queries,
                relationship=relationship,
                settings=submit_settings,
                parent_type=parent_type,
                parent_sha256=parent_sha256,
                filename=filename,
            ),
            api="/api/v0/binaries/child",
            file_path_or_contents=file_path_or_contents,
            filename=filename,
            extract=extract,
            password=password,
            refresh=refresh,
        )

    def expedite_processing(self, sha256: str, *, bypass_cache: bool = False) -> None:
        """Expedite or reprocess the file with the provided sha256.

        If bypass_cache is on ensure plugins actually re-process the binary and don't rely on the cache.
        """
        self._request(
            method=HTTPMethod.POST,
            url=self.cfg.azul_url + f"/api/v0/binaries/{sha256}/expedite",
            params={"bypass_cache": bypass_cache},
        )

    def download_augmented_stream(self, sha256: str, stream_sha256: str):
        """Download the raw augmented stream for a given submission binary.

        First sha256 is the sha256 of the binary and the second sha256 is that of the augmented stream.
        """
        return self._request(
            method=HTTPMethod.GET,
            url=self.cfg.azul_url + f"/api/v0/binaries/{sha256}/content/{stream_sha256}",
        ).content

    def download_hex(
        self, sha256, *, offset: int = 0, max_bytes_to_read: int | None = None, shortform: bool = False
    ) -> models_restapi.BinaryHexView:
        """Download either all or a section of the raw hex of a file.

        Typically used in chunks to download sections of a file until either you have the whole file or have
        enough information from the file.

        :param str sha256: sha256 of the file you want to download hex for.
        :param int offset: starting offset for where to download the hex from.
        :param int max_bytes_to_read: bytes to read before stopping and returning what you have.
        :param bool shortform: If true, will return 16 hex bytes as a string instead of 16 strings in a list.
        """
        params = {"offset": offset, "max_bytes_to_read": max_bytes_to_read, "shortform": shortform}
        params = self.filter_none_values(params)

        return self._request_with_pydantic_model_response(
            method=HTTPMethod.GET,
            url=self.cfg.azul_url + f"/api/v0/binaries/{sha256}/hexview",
            response_model=models_restapi.BinaryHexView,
            params=params,
        )

    def get_strings(
        self,
        sha256: str,
        *,
        min_length: int = 4,
        max_length: int = 200,
        offset: int = 0,
        max_bytes_to_read: int = DEFAULT_MAX_BYTES_TO_READ,
        take_n_strings: int = 1000,
        filter: str | None = None,
        regex: str | None = None,
        file_format_legacy: str | None = None,
    ) -> models_restapi.BinaryStrings:
        """Get strings for a binary file with multiple potential additional parameters.

        :param str sha256: File to get strings for.
        :param int min_length: Minimum length of string (when decoded).
        :param int max_length: Maximum length of string (when decoded).
        :param int offset: Search for strings from offset.
        :param int max_bytes_to_read: How many bytes to search for, default of 10MB.
        :param int take_n_strings: ow many strings to return.
        :param str filter: Case-insensitive search string to filter strings with.
        :param str regex: Regex pattern to search strings with.
        :param str file_format_legacy: Optional file type for AI string filter.
        """
        params = {
            "min_length": min_length,
            "max_length": max_length,
            "offset": offset,
            "max_bytes_to_read": max_bytes_to_read,
            "take_n_strings": take_n_strings,
            "filter": filter,
            "regex": regex,
            "file_format_legacy": file_format_legacy,
        }
        params = self.filter_none_values(params)

        return self._request_with_pydantic_model_response(
            method=HTTPMethod.GET,
            url=self.cfg.azul_url + f"/api/v0/binaries/{sha256}/strings",
            response_model=models_restapi.BinaryStrings,
            params=params,
        )

    def search_hex(
        self,
        sha256: str,
        filter: str,
        *,
        offset: int = 0,
        max_bytes_to_read: int | None = None,
        take_n_hits: int = 1000,
    ) -> models_restapi.BinaryStrings:
        """Search a hex file with the given filter.

        :param str sha256: Search the data file that has this sha256.
        :param str filter: Search a hex file with the given hex string filter.
        :param int offset: Search for hits from offset.
        :param int max_bytes_to_read: How many bytes to search for, if this is not set, return to EOF.
        :param int take_n_hits: Maximum number of hits to return.
        """
        params = {
            "offset": offset,
            "max_bytes_to_read": max_bytes_to_read,
            "take_n_hits": take_n_hits,
            "filter": filter,
        }
        params = self.filter_none_values(params)

        return self._request_with_pydantic_model_response(
            method=HTTPMethod.GET,
            url=self.cfg.azul_url + f"/api/v0/binaries/{sha256}/search/hex",
            response_model=models_restapi.BinaryStrings,
            params=params,
        )
