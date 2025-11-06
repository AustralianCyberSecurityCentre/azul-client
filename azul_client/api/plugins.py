"""Mappings for the Azul plugins API."""

import logging
from http import HTTPMethod

from azul_bedrock import models_restapi
from pydantic import TypeAdapter

from azul_client.api.base_api import BaseApiHandler

logger = logging.getLogger(__name__)

latest_plugin_list_adapter = TypeAdapter(list[models_restapi.LatestPluginWithVersions])
latest_plugin_status_list_adapter = TypeAdapter(list[models_restapi.PluginStatusSummary])


class Plugins(BaseApiHandler):
    """API for listing out the plugins currently running in Azul, their configuration and latest statuses."""

    def get_all_plugins(self) -> list[models_restapi.LatestPluginWithVersions]:
        """Read names and versions of all registered plugins."""
        return self._request_with_pydantic_model_response(
            url=self.cfg.azul_url + "/api/v0/plugins",
            method=HTTPMethod.GET,
            response_model=latest_plugin_list_adapter,
            get_data_only=True,
        )

    def get_all_plugin_statuses(self) -> list[models_restapi.PluginStatusSummary]:
        """Read names and versions of all registered plugins and count statuses.

        Note - the status count is inaccurate because it doesn't filter out duplicates.
        A duplicate is where the same binary is submitted to a plugin with a different path.
        """
        return self._request_with_pydantic_model_response(
            url=self.cfg.azul_url + "/api/v0/plugins/status",
            method=HTTPMethod.GET,
            response_model=latest_plugin_status_list_adapter,
            get_data_only=True,
        )

    def get_plugin(self, name: str, version: str) -> models_restapi.PluginInfo:
        """Get all the configuration information about a specific plugin by name and version."""
        return self._request_with_pydantic_model_response(
            url=self.cfg.azul_url + f"/api/v0/plugins/{name}/versions/{version}",
            method=HTTPMethod.GET,
            response_model=models_restapi.PluginInfo,
            get_data_only=True,
        )
