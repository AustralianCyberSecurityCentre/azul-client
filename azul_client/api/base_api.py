"""Base API class used by all other Azul API classes."""

import json
import logging
from http import HTTPMethod
from typing import Any, Callable, TypeVar

import httpx
from azul_bedrock import models_restapi
from pydantic import BaseModel, TypeAdapter

from azul_client import config, exceptions

T = TypeVar("T", bound=BaseModel)


class BaseApiHandler:
    """Base class for handling all restapi calls with generic functionality."""

    def __init__(self, cfg: config.Config, get_client: Callable[[], httpx.Client]):
        self.logger = logging.getLogger(type(self).__name__)
        self.cfg = cfg
        self.__get_client = get_client
        self._last_meta = None
        self.__excluded_security = None

    @property
    def _excluded_security(self) -> list[str]:
        """Get excluded security (recommended to get this at the API level)."""
        return self.__excluded_security

    @_excluded_security.setter
    def _excluded_security(self, security_list: list[str]):
        """Set excluded security's to the provided values (internal use only get directly get from API level)."""
        self.__excluded_security = security_list

    def get_meta_from_last_request(self) -> models_restapi.Meta | None:
        """Get the metadata from the last request that was made, if there is no metadata return None."""
        if self._last_meta:
            return models_restapi.Meta.model_validate(self._last_meta)
        return None

    def __request_to_client(
        self,
        method: HTTPMethod,
        url: str,
        params: httpx.QueryParams | dict | None = None,
        json: Any = None,
        timeout: int | None = None,
    ) -> httpx.Response:
        if timeout is None:
            timeout = httpx.USE_CLIENT_DEFAULT
        if self._excluded_security:
            if not params:
                params = dict()
            params["x"] = self._excluded_security
        if method == HTTPMethod.GET:
            if json is not None:
                raise ValueError("Get request cannot accept a body parameter.")
            return self.__get_client().get(url, params=params, timeout=timeout)
        elif method == HTTPMethod.POST:
            return self.__get_client().post(url, params=params, json=json, timeout=timeout)
        elif method == HTTPMethod.DELETE:
            if json is not None:
                raise ValueError("Delete request cannot accept a body parameter.")
            return self.__get_client().delete(url, params=params, timeout=timeout)
        raise ValueError(
            f"The provided method '{method}' is invalid it must be one of "
            + f"{', '.join([HTTPMethod.POST, HTTPMethod.POST])}"
        )

    def _request_with_pydantic_model_response(
        self,
        *,
        method: HTTPMethod,
        url: str,
        response_model: type[T],
        params: httpx.QueryParams | dict | None = None,
        json: Any = None,
        get_data_only: bool = False,
        timeout: int | None = None,
    ) -> T:
        """Generic Handler for requests.

        :param HTTPMethod method: HTTP method to use only GET and POST are supported.
        :param str url: url to post or get to.
        :param BaseModel response_model: Expected Pydantic response model.
        :param httpx.QueryParams params: parameters for the request.
        :param Any json: raw body that will be json encoded and sent with POST request, doesn't work with get requests.
        :param bool get_data_only: will extract out the 'data' key from the response and throws an
        exception if there is no data field.
        """
        # API requests should always clear last requests metadata
        self._last_meta = None
        resp = self.__request_to_client(method=method, url=url, params=params, json=json, timeout=timeout)

        if resp.status_code != 200 and resp.status_code != 206:
            raise exceptions.bad_response(resp)

        raw_content: str | bytes = resp.content
        if get_data_only:
            raw_content = self._get_response_data(resp)

        try:
            if isinstance(response_model, TypeAdapter):
                return response_model.validate_json(raw_content)
            return response_model.model_validate_json(raw_content)
        except Exception:
            self.logger.error(f"Failed to deserialize pydantic model {type(response_model)}.")
            self.logger.error(f"Response started with {raw_content[:500]}")
            raise

    def _request(
        self,
        *,
        method: HTTPMethod,
        url: str,
        params: httpx.QueryParams = None,
        json: Any = None,
        timeout: int | None = None,
    ) -> httpx.Response:
        """Send a http request with the provided method to azul, and provide the json response."""
        # API requests should always clear last requests metadata
        self._last_meta = None
        resp = self.__request_to_client(method=method, url=url, params=params, json=json, timeout=timeout)

        if resp.status_code != 200 and resp.status_code != 206:
            raise exceptions.bad_response(resp)
        return resp

    def _request_upload(self, *, url: str, params: dict, files: dict, data: dict, timeout: int) -> httpx.Response:
        """Special request type for uploading files."""
        # API requests should always clear last requests metadata
        self._last_meta = None
        return self.__get_client().post(url=url, params=params, files=files, data=data, timeout=timeout)

    def _generic_head_request(self, url: str) -> bool:
        """Generic request to check if a resource exists."""
        # API requests should always clear last requests metadata
        self._last_meta = None
        resp = self.__get_client().head(url)
        if resp.status_code == 404:
            return False
        if resp.status_code != 200 and resp.status_code != 206:
            raise exceptions.bad_response(resp)
        return True

    def _get_response_data(self, resp: httpx.Response) -> str:
        """Get the 'data' key from a response when it may also have a 'metadata' field."""
        json_response = resp.json()
        data = json_response.get("data", None)
        self._last_meta = json_response.get("meta", None)
        if data is None:
            raise Exception("Response has no 'data' key and 'data cannot be extracted.")

        return json.dumps(data)

    def filter_none_values(self, params: dict) -> dict:
        """Takes a dictionary and filters out all keys with None values."""
        for k, v in list(params.items()):
            if v is None:
                params.pop(k)
        return params
