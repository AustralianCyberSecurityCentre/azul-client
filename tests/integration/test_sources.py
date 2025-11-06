from azul_bedrock import models_restapi
from azul_bedrock import models_settings as azs

from azul_client.exceptions import BadResponse404

from .base_test import BaseApiTest


class TestSourcesApi(BaseApiTest):
    def test_get_all_sources(self):
        """Verify sources exist and are in the appropriate form."""
        dict_of_sources = self.api.sources.get_all_sources()
        print(dict_of_sources)
        self.assertIsInstance(dict_of_sources, dict)
        source_name_list = list(dict_of_sources.keys())
        # Should be 3 or more sources.
        self.assertIn(self.known_source, source_name_list)
        self.assertGreater(len(source_name_list), 3)
        self.assertIsInstance(source_name_list[0], str)

        self.assertIsInstance(dict_of_sources[self.known_source], azs.Source)
        # Verify the description is not the default meaning it was set to something.
        self.assertGreater(len(dict_of_sources[self.known_source].description), 2)

    def test_get_meta_for_all_sources(self):
        """Verify meta data is gathered correctly."""
        self.api.sources.get_all_sources()
        meta = self.api.sources.get_meta_from_last_request()
        self.assertIsInstance(meta, models_restapi.Meta)
        # Verify we have some kind of security string
        self.assertGreater(len(meta.security), 2)

    def test_check_source_exists(self):
        """Verify that the requested sources either exist or don't."""
        does_sort_exist = self.api.sources.check_source_exists(self.known_source)
        self.assertTrue(does_sort_exist)

        # Verify a random source name doesn't exist
        does_sort_exist = self.api.sources.check_source_exists("JSDLKFJVIEURNDCKdmf")
        self.assertFalse(does_sort_exist)

    def test_read_source(self):
        source_info = self.api.sources.read_source(self.known_source)
        self.assertIsInstance(source_info, models_restapi.Source)
        # Verify there is some data in the source.
        self.assertGreaterEqual(source_info.num_entities, 2)
        self.assertEqual(source_info.name, self.known_source)

    def test_read_source_references(self):
        source_ref = self.api.sources.read_source_references(self.known_source)
        self.assertIsInstance(source_ref, models_restapi.References)
        self.assertGreater(len(source_ref.items), 1)

        with self.assertRaises(BadResponse404):
            self.api.sources.read_source_references(self.known_source, term="termquery")

        source_ref = self.api.sources.read_source_references(self.known_source, term=self.test_username)
        self.assertIsInstance(source_ref, models_restapi.References)
        self.assertGreaterEqual(len(source_ref.items), 1)
