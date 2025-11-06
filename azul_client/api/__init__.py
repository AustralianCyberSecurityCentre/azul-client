"""Api wrapper."""

import logging
import sys

from azul_client import config, oidc

from . import (
    binaries_data,
    binaries_meta,
    features,
    plugins,
    purge,
    security,
    sources,
    statistics,
    users,
)

logging.basicConfig(stream=sys.stdout, level=logging.WARNING)


class Api:
    """Contains api implementation instances.

    This is so we can chain different API calls without having to
    juggle multiple objects.
    """

    def __init__(self, conf: config.Config | None = None) -> None:
        # api support
        self.config = conf if conf else config.get_config()
        self.auth = oidc.OIDC(self.config)

        self._api_implementations = []
        # api implementations
        self.binaries_data = binaries_data.BinariesData(self.config, self.auth.get_client)
        self._api_implementations.append(self.binaries_data)

        self.binaries_meta = binaries_meta.BinariesMeta(self.config, self.auth.get_client)
        self._api_implementations.append(self.binaries_meta)

        self.features = features.Features(self.config, self.auth.get_client)
        self._api_implementations.append(self.features)

        self.plugins = plugins.Plugins(self.config, self.auth.get_client)
        self._api_implementations.append(self.plugins)

        self.purge = purge.Purge(self.config, self.auth.get_client)
        self._api_implementations.append(self.purge)

        self.security = security.Security(self.config, self.auth.get_client)
        self._api_implementations.append(self.security)

        self.sources = sources.Sources(self.config, self.auth.get_client)
        self._api_implementations.append(self.sources)

        self.statistics = statistics.Statistics(self.config, self.auth.get_client)
        self._api_implementations.append(self.statistics)

        self.users = users.Users(self.config, self.auth.get_client)
        self._api_implementations.append(self.users)

    def get_excluded_security(self) -> list[str]:
        """Get excluded security."""
        return self._excluded_security

    def set_excluded_security(self, security_list: list[str]):
        """Set excluded security's to the provided values."""
        self._excluded_security = security_list

        # Set excluded security for all API's
        for api in self._api_implementations:
            api._excluded_security = self._excluded_security
