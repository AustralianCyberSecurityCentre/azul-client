"""Mappings for the Azul purge API."""

import logging
from http import HTTPMethod

import pendulum
from azul_bedrock import models_restapi
from pendulum.parsing.exceptions import ParserError

from azul_client.api.base_api import BaseApiHandler

logger = logging.getLogger(__name__)


class Purge(BaseApiHandler):
    """API for purging files out of Azul, this API requires a user who is an admin to use it.

    NOTE - Purges should not be scripted as standard practice, they have a high cost to Azul
    and too many purges or frequent re-purging will cause instability in Azul.
    """

    def purge_submission(
        self, track_source_references: str, *, timestamp: str, purge: bool = False
    ) -> models_restapi.PurgeSimulation | models_restapi.PurgeResults:
        """Purge a set of submissions (requires admin privileges).

        :param str source_data_kvs: Unique identifier for the submission set (submission source + all metadata).
        :param str binary: Sha256 Id of the binary to purge from the submission (optional).
        :param str timestamp: Timestamp of the submission to purge (optional).
        :param bool purge: If true, perform the purge instead of a simulation (default False).
        """
        params = {"purge": purge}
        if not timestamp:
            raise ValueError("Timestamp is required to be set and cannot be None or and empty string.")
        try:
            pendulum.parse(timestamp)
        except ParserError:
            raise ValueError(f"Timestamp has an invalid value '{timestamp}' must be a")
        params["timestamp"] = timestamp

        if purge:
            response_model = models_restapi.PurgeResults
        else:
            response_model = models_restapi.PurgeSimulation

        return self._request_with_pydantic_model_response(
            url=self.cfg.azul_url + f"/api/v0/purge/submission/{track_source_references}",
            method=HTTPMethod.DELETE,
            response_model=response_model,
            params=params,
            get_data_only=True,
        )

    def purge_link(
        self, track_link: str, *, purge: bool = False
    ) -> models_restapi.PurgeSimulation | models_restapi.PurgeResults:
        """Purge a manually added relationship between binaries."""
        params = {"purge": purge}

        if purge:
            response_model = models_restapi.PurgeResults
        else:
            response_model = models_restapi.PurgeSimulation

        return self._request_with_pydantic_model_response(
            url=self.cfg.azul_url + f"/api/v0/purge/link/{track_link}",
            method=HTTPMethod.DELETE,
            response_model=response_model,
            params=params,
            get_data_only=True,
        )
