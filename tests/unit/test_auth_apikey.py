from azul_client import Config
from unittest import mock
import unittest
from azul_client.oidc import OIDC


def _mock_save(x):
    pass


class TestApiKeyAuth(unittest.TestCase):
    """Setting header via API key based auth."""

    def test_api_key_auth(self):
        """Test API key based authentication."""
        dummy_api_key = "dummy-api-key"
        api_key_auth = OIDC(Config(auth_type="apikey", api_key=dummy_api_key))

        client = api_key_auth.get_client()
        headers = client.headers
        self.assertEqual(headers.get("X-API-Key"), dummy_api_key)
