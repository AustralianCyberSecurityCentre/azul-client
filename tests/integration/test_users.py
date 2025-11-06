from azul_bedrock import models_restapi
from azul_bedrock.models_auth import UserInfo

from .base_test import BaseApiTest


class TestUsersApi(BaseApiTest):
    def setUp(self):
        super().setUp()

        # Ensure this is unset between runs
        self.api.set_excluded_security(None)

    def test_get_security_settings(self):
        """Verify UserAccess is being acquired correctly from the API."""
        opensearch_user_access = self.api.users.get_opensearch_user_info()
        self.assertIsInstance(opensearch_user_access, models_restapi.UserAccess)
        print(opensearch_user_access)
        # Verify a role is in opensearch that is unlikely to change.
        self.assertIn("azul_read", opensearch_user_access.roles)

    def test_meta_is_none_appropriately(self):
        """Verify metadata is set to none for a query that doesn't set it."""
        self.api.users.get_opensearch_user_info()
        meta = self.api.sources.get_meta_from_last_request()
        self.assertIsNone(meta)

    def test_get_user_infos(self):
        """Verify UserInfo is being acquired correctly from the API."""
        user_info = self.api.users.get_user_info()
        print(user_info)
        self.assertIsInstance(user_info, UserInfo)
        # Verify a common user_info role is present.
        self.assertIn("azul_read", user_info.roles)

    def test_get_security_settings_with_excluded_security(self):
        """Test excluded security doesn't cause an issue on an API endpoint that doesn't use it."""
        for class_to_exclude in [self.min_security, self.max_security, self.default_security]:
            self.api.set_excluded_security([class_to_exclude])
            opensearch_user_access = self.api.users.get_opensearch_user_info()
            self.assertIsInstance(opensearch_user_access, models_restapi.UserAccess)
            # Verify a role is in opensearch that is unlikely to change.
            self.assertIn("azul_read", opensearch_user_access.roles)

    def test_get_user_infos_with_excluded_security(self):
        """Test exclude security works when it's meant to exclude security information."""
        for class_to_exclude in [self.min_security, self.max_security, self.default_security]:
            self.api.set_excluded_security([class_to_exclude])
            user_info = self.api.users.get_user_info()
            self.assertIsInstance(user_info, UserInfo)
            # Verify a common user_info role is present.
            self.assertIn("azul_read", user_info.roles)
