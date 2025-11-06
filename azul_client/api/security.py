"""Module for interacting with security endpoints."""

from http import HTTPMethod

from azul_client.api.base_api import BaseApiHandler


class Security(BaseApiHandler):
    """Interact with binary endpoints."""

    def get_security_settings(self) -> dict:
        """Get the current Azul security settings."""
        return self._request(method=HTTPMethod.GET, url=self.cfg.azul_url + "/api/v0/security").json()

    def normalise(self, security: str) -> str:
        """Validates and normalises a security string."""
        return self._request(
            method=HTTPMethod.POST, url=self.cfg.azul_url + "/api/v1/security/normalise", json={"security": security}
        ).json()

    def get_max_security_string(self, security_strings: list[str]) -> str:
        """Get the max security from the provided security strings."""
        return self._request(
            method=HTTPMethod.POST, url=self.cfg.azul_url + "/api/v1/security/max", json=security_strings
        ).json()

    def get_is_user_an_admin(self) -> bool:
        """Get a boolean value indicating if the authenticated user is an admin or not."""
        return self._request(method=HTTPMethod.GET, url=self.cfg.azul_url + "/api/v0/security/is_admin").json()
