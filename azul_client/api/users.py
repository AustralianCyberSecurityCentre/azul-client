"""Mappings for the Azul user API."""

import logging
from http import HTTPMethod

from azul_bedrock import models_restapi
from azul_bedrock.models_auth import UserInfo

from azul_client.api.base_api import BaseApiHandler

logger = logging.getLogger(__name__)


class Users(BaseApiHandler):
    """API for accessing user information of Azul."""

    def get_opensearch_user_info(self) -> models_restapi.UserAccess:
        """Get Opensearch User info through Azul's restapi."""
        return self._request_with_pydantic_model_response(
            url=self.cfg.azul_url + "/api/v0/users/me/opensearch",
            method=HTTPMethod.GET,
            response_model=models_restapi.UserAccess,
        )

    def get_user_info(self) -> UserInfo:
        """Get user information for the current Token that Azul cares about."""
        return self._request_with_pydantic_model_response(
            url=self.cfg.azul_url + "/api/v0/users/me", method=HTTPMethod.GET, response_model=UserInfo
        )
