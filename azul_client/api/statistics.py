"""Mappings for the Azul statistics API."""

import logging
from http import HTTPMethod

from azul_bedrock import models_restapi

from azul_client.api.base_api import BaseApiHandler

logger = logging.getLogger(__name__)


class Statistics(BaseApiHandler):
    """API for accessing statistics about Azul from the Azul restapi."""

    def get_statistics(self) -> models_restapi.StatisticSummary:
        """Get statistics about the current state of azul."""
        return self._request_with_pydantic_model_response(
            url=self.cfg.azul_url + "/api/v0/statistics",
            method=HTTPMethod.GET,
            response_model=models_restapi.StatisticSummary,
            get_data_only=True,
        )
