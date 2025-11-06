from azul_client.exceptions import BadResponse, BadResponse404

from .base_test import BaseApiTest


class TestSecurityApi(BaseApiTest):
    def test_get_security_settings(self):
        """Verify security settings are a dictionary and have some basic keys."""
        resp = self.api.security.get_security_settings()
        print(resp)
        self.assertIsInstance(resp, dict)
        # Verify some of the keys you would expect to find in security settings are there.
        self.assertIn("labels", resp)
        self.assertIn("default", resp)

    def test_normalise_basic(self):
        """Minimal tests in case security changes."""
        resp_content = self.api.security.normalise("OFFICIAL      TLP:GREEN")
        self.assertEqual(resp_content, "OFFICIAL TLP:GREEN")

        resp_content = self.api.security.normalise("OFFICIAL TLP:GREEN")
        self.assertEqual(resp_content, "OFFICIAL TLP:GREEN")

        resp_content = self.api.security.normalise("OFFICIAL//TLP:GREEN")
        self.assertEqual(resp_content, "OFFICIAL TLP:GREEN")

        resp_content = self.api.security.normalise("LESS OFFICIAL//TLP:CLEAR")
        self.assertEqual(resp_content, "LESS OFFICIAL TLP:CLEAR")

        resp_content = self.api.security.normalise("MORE OFFICIAL//REL:APPLE")
        self.assertEqual(resp_content, "MORE OFFICIAL REL:APPLE")

        self.assertRaises(BadResponse, self.api.security.normalise, "OFFICIAL//TLP:GREEN//REL:APPLE")
        self.assertRaises(BadResponse, self.api.security.normalise, "OFFICIAL//REL:APPLE")
        self.assertRaises(BadResponse, self.api.security.normalise, "OFFICIAL AMBER")
        self.assertRaises(BadResponse, self.api.security.normalise, "OFF")
        self.assertRaises(BadResponse, self.api.security.normalise, "TLP:GREEN")
        self.assertRaises(BadResponse, self.api.security.normalise, "REL:APPLE")

    def test_get_max_security_string(self):
        max_security = self.api.security.get_max_security_string(["OFFICIAL TLP:GREEN"])
        self.assertEqual(max_security, "OFFICIAL TLP:GREEN")
        max_security = self.api.security.get_max_security_string(
            ["OFFICIAL TLP:CLEAR", "OFFICIAL TLP:GREEN", "OFFICIAL TLP:AMBER"]
        )
        self.assertEqual(max_security, "OFFICIAL TLP:AMBER")
        max_security = self.api.security.get_max_security_string(["MORE OFFICIAL REL:APPLE"])
        self.assertEqual(max_security, "MORE OFFICIAL REL:APPLE")
        with self.assertRaises(Exception):
            self.api.security.get_max_security_string("OFFICIAL//TLP:CLEAR")

        with self.assertRaises(Exception):
            self.api.security.get_max_security_string("REL:APPLE")

    def test_get_is_user_an_admin(self):
        """Verify the test user is an admin user."""
        is_user_admin = self.api.security.get_is_user_an_admin()
        self.assertIsInstance(is_user_admin, bool)
        self.assertTrue(is_user_admin)
