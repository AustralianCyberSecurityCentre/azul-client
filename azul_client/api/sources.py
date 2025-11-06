"""Mappings for the Azul sources API."""

import logging
from http import HTTPMethod

from azul_bedrock import models_restapi
from azul_bedrock import models_settings as azs
from pydantic import TypeAdapter

from azul_client.api.base_api import BaseApiHandler

logger = logging.getLogger(__name__)


source_dict_type_adapter = TypeAdapter(dict[str, azs.Source])


class Sources(BaseApiHandler):
    """API for accessing information about the sources configured in Azul."""

    def get_all_sources(self) -> dict[str, azs.Source]:
        """Get a list of all sources from Azul."""
        return self._request_with_pydantic_model_response(
            url=self.cfg.azul_url + "/api/v0/sources",
            method=HTTPMethod.GET,
            response_model=source_dict_type_adapter,
            get_data_only=True,
        )

    def check_source_exists(self, source_id: str) -> bool:
        """Check if the provided source exists in Azul."""
        return self._generic_head_request(self.cfg.azul_url + f"/api/v0/sources/{source_id}")

    def read_source(self, source_id: str) -> models_restapi.Source:
        """Read the information for the requested source."""
        return self._request_with_pydantic_model_response(
            url=self.cfg.azul_url + f"/api/v0/sources/{source_id}",
            method=HTTPMethod.GET,
            response_model=models_restapi.Source,
            get_data_only=True,
        )

    def read_source_references(self, source_id: str, *, term: str = "") -> models_restapi.References:
        """Read source references for a source_id with an optional term query."""
        return self._request_with_pydantic_model_response(
            url=self.cfg.azul_url + f"/api/v0/sources/{source_id}/references",
            method=HTTPMethod.GET,
            response_model=models_restapi.References,
            params={"term": term},
            get_data_only=True,
        )
