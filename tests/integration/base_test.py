"""Base test case used to pass common variables and test functionalities to all test cases."""

import copy
import unittest

from azul_client.api import Api
from azul_client.config import get_config

from . import module_ref, module_sha256s, module_source, upload_security


class BaseApiTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.api = Api(get_config())
        # A known source that should always be there
        cls.known_source = module_source
        # Sha256's
        cls.sha256s = copy.copy(module_sha256s)
        cls.upload_ref = module_ref
        # Name of a plugin unlikely to ever be removed
        cls.entropy_plugin_name = "Entropy"
        # Name of a plugin unlikely to ever be removed that is python based.
        cls.python_known_plugin_name = "Alphabets"
        # Name of the test user.
        cls.test_username = module_ref.get("user")
        # Default level of security to assign to new objects during testing.
        cls.default_security = upload_security
        cls.max_security = "MORE OFFICIAL"
        cls.min_security = "LESS OFFICIAL"

        cls.non_existent_sha256 = "ed7002b439e9ac845f22357d822bac1444730fbdb6016d3ec9432297b9ec9f74"
        super().setUpClass()
