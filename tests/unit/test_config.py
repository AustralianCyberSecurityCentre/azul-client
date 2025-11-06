import os
import tempfile
from unittest import TestCase

from azul_client import config as mconfig


class BaseConfigTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        # Delete all environment variables
        cls.original_env = dict(os.environ)
        for env_key in cls.original_env.keys():
            del os.environ[env_key]

    @classmethod
    def tearDownClass(cls):
        # Set all environment variables back to what they were.
        for env_key, env_value in cls.original_env.items():
            os.environ[env_key] = env_value


class TestClient(BaseConfigTestCase):
    def tearDown(self):
        if os.environ.get("AZUL_CONFIG_LOCATION"):
            del os.environ["AZUL_CONFIG_LOCATION"]

    def test_get(self):
        with tempfile.NamedTemporaryFile("w") as f:
            f.write(
                """
[default]
azul_url = http://lemon
            """
            )
            f.seek(0)
            print(f.name)
            os.environ["AZUL_CONFIG_LOCATION"] = f.name
            conf = mconfig.get_config()

        self.assertEqual(conf.azul_url, "http://lemon")
        self.assertEqual(conf.auth_type, "callback")

        with tempfile.NamedTemporaryFile("w") as f:
            f.write(
                """
[default]
azul_url = http://lemon/
            """
            )
            f.seek(0)
            print(f.name)
            os.environ["AZUL_CONFIG_LOCATION"] = f.name
            conf = mconfig.get_config()

        self.assertEqual(conf.azul_url, "http://lemon")
        self.assertEqual(conf.auth_type, "callback")

        with tempfile.NamedTemporaryFile("w") as f:
            f.write(
                """
[default]
azul_url = http://lemon
oidc_url = here
auth_type = blah
auth_client_id = there
            """
            )
            f.seek(0)
            print(f.name)
            os.environ["AZUL_CONFIG_LOCATION"] = f.name
            conf = mconfig.get_config()

        self.assertEqual(conf.azul_url, "http://lemon")
        self.assertEqual(conf.oidc_url, "here")
        self.assertEqual(conf.auth_type, "blah")
        self.assertEqual(conf.auth_client_id, "there")


class TestLocation(TestCase):
    def setUp(self):
        if os.environ.get("AZUL_CONFIG_LOCATION"):
            del os.environ["AZUL_CONFIG_LOCATION"]

    def tearDown(self):
        if os.environ.get("AZUL_CONFIG_LOCATION"):
            del os.environ["AZUL_CONFIG_LOCATION"]

    def test_location_default(self):
        self.assertTrue(
            mconfig.ConfigLocation().azul_config_location.endswith(".azul.ini"),
            msg=f"{mconfig.ConfigLocation().azul_config_location} does not end with .azul.ini",
        )
        os.environ["AZUL_CONFIG_LOCATION"] = "/tmp/azul.ini"
        self.assertEqual(mconfig.ConfigLocation().azul_config_location, "/tmp/azul.ini")
