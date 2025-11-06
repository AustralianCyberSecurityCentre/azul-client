from azul_bedrock import models_restapi

from .base_test import BaseApiTest


class TestStatisticsApi(BaseApiTest):
    def test_get_statistics(self):
        """Verify UserAccess is being acquired correctly from the API."""
        resp = self.api.statistics.get_statistics()
        self.assertGreaterEqual(resp.binary_count, 2)

    def test_meta_is_set_appropriately(self):
        """Verify metadata is set to a value for stats which should set it."""
        self.api.statistics.get_statistics()
        meta = self.api.statistics.get_meta_from_last_request()
        self.assertIsInstance(meta, models_restapi.Meta)
