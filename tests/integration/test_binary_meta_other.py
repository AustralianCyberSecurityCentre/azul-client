import time

import pendulum
from azul_bedrock import models_network as azm
from azul_bedrock import models_restapi
from azul_bedrock.models_restapi import binaries_auto_complete as bedr_bauto

from azul_client.exceptions import BadResponse, BadResponse404

from .base_test import BaseApiTest


class TestBinaryMetaOther(BaseApiTest):
    @classmethod
    def setUpClass(cls):
        cls.non_existent_sha256 = "ed7002b439e9ac845f22357d822bac1444730fbdb6016d3ec9432297b9ec9f74"
        cls.tag_to_create = "test-tag"
        super().setUpClass()

    def test_verify_sha256s_length(self):
        """Ensure the sha256's are the appropriate length.

        This covers the tests that loop over sha256 to make sure their assertions are actually getting called.
        For the actual length refer to the module setup in __init__
        """
        self.assertGreaterEqual(len(self.sha256s), 10)

    def test_check_meta(self):
        for sha in self.sha256s:
            exists = self.api.binaries_meta.check_meta(sha)
            self.assertEqual(exists, True)

        exists = self.api.binaries_meta.check_meta(self.non_existent_sha256)
        self.assertEqual(exists, False)

    def test_get_meta(self):
        first = self.sha256s[0]
        meta = self.api.binaries_meta.get_meta(first, bucket_size=1000)

        source_ids: list[str] = []
        references: list[dict] = []
        for source in meta.sources:
            for d_source in source.direct:
                source_ids.append(d_source.name)
                references.append(d_source.references)

        self.assertIn(self.known_source, source_ids)
        self.assertIn(self.upload_ref, references)

        # test non-existent file
        self.assertRaises(BadResponse404, self.api.binaries_meta.get_meta, self.non_existent_sha256)

        self.assertIn(self.known_source, source_ids)
        self.assertIn(self.upload_ref, references)

        meta_less = self.api.binaries_meta.get_meta(
            first, details=[models_restapi.BinaryMetadataDetail.documents], bucket_size=1000
        )
        self.assertEqual(0, len(meta_less.sources))

        # The detailed version of the binaries metadata should include sufficient additional information
        # to be at least x characters longer than the original metadata
        simple = meta_less.model_dump_json(exclude_unset=True, exclude_defaults=True)
        complex = meta.model_dump_json(exclude_unset=True, exclude_defaults=True)
        self.assertLess(len(simple) + 200, len(complex))

    def test_get_model(self):
        model = self.api.binaries_meta.get_model()
        self.assertIsInstance(model, models_restapi.EntityModel)
        # There should be some keys.
        self.assertGreater(len(model.keys), 1)

    def test_find_autocomplete(self):
        # No data yet
        resp = self.api.binaries_meta.find_autocomplete("")
        print(resp)
        self.assertEqual(resp.type, "Initial")
        self.assertIsInstance(resp, bedr_bauto.AutocompleteInitial)

        # Results
        resp = self.api.binaries_meta.find_autocomplete("a")
        print(resp)
        self.assertEqual(resp.type, "FieldValue")
        self.assertIsInstance(resp, bedr_bauto.AutocompleteFieldValue)
        resp = self.api.binaries_meta.find_autocomplete("b")
        print(resp)
        self.assertEqual(resp.type, "FieldValue")
        self.assertIsInstance(resp, bedr_bauto.AutocompleteFieldValue)

        # No results
        resp = self.api.binaries_meta.find_autocomplete("kjhasdfhjkkjhdfskjhasfdkjh")
        self.assertEqual(resp.type, "FieldValue")
        self.assertIsInstance(resp, bedr_bauto.AutocompleteFieldValue)

        # Trailing Space causing None return type.
        resp = self.api.binaries_meta.find_autocomplete("dnksjvdkjndvs  ")
        self.assertEqual(resp.type, "None")
        self.assertIsInstance(resp, bedr_bauto.AutocompleteNone)
        print(resp)

        # Invalid input for auto-completion
        resp = self.api.binaries_meta.find_autocomplete("$@!!@#$%^&*()")
        print(resp)
        self.assertEqual(resp.type, "Error")
        self.assertIsInstance(resp, bedr_bauto.AutocompleteError)

    def test_get_has_newer_metadata(self):
        # Check if there has been any new data an hour in the future (this should be impossible)
        now = pendulum.now()
        now_plus_an_hour = now + pendulum.duration(hours=1)
        resp = self.api.binaries_meta.get_has_newer_metadata(self.sha256s[0], now_plus_an_hour.isoformat())
        self.assertEqual(resp.count, 0)

        # Check if there has been any new data ever. (there should be)
        forever_ago = pendulum.datetime(2000, 1, 1, 1, 1, 1, 1, tz=pendulum.UTC)
        resp = self.api.binaries_meta.get_has_newer_metadata(self.sha256s[0], forever_ago.isoformat())
        # verify at least one new piece of data.
        self.assertGreater(resp.count, 0)

    def test_get_similar_ssdeep_entities(self):
        # Empty ssdeep should create a value error
        with self.assertRaises(ValueError):
            self.api.binaries_meta.get_similar_ssdeep_entities("")

        # Invalid ssdeep should cause an API exception
        with self.assertRaises(BadResponse):
            self.api.binaries_meta.get_similar_ssdeep_entities("hkjdsfhjksdf")

        first = self.sha256s[0]
        binary_list = self.api.binaries_meta.find_hashes(hashes=[first])
        self.assertEqual(len(binary_list.items), 1)
        first_binary_data = binary_list.items[0]
        first_binary_ssdeep = first_binary_data.ssdeep
        resp = self.api.binaries_meta.get_similar_ssdeep_entities(ssdeep=first_binary_ssdeep)
        # Should be at least a couple of matches because we generate a lot of very similar files.
        self.assertGreater(len(resp.matches), 4)

    def test_get_similar_tlsh_entities(self):
        # Empty tlsh should create a value error
        with self.assertRaises(ValueError):
            self.api.binaries_meta.get_similar_tlsh_entities("")

        # Invalid tlsh (v9 doesn't exist) should cause an API exception
        with self.assertRaises(BadResponse):
            self.api.binaries_meta.get_similar_tlsh_entities("T9jdsfhjksdf")

        first = self.sha256s[0]
        binary_list = self.api.binaries_meta.find_hashes(hashes=[first])
        self.assertEqual(len(binary_list.items), 1)
        first_binary_data = binary_list.items[0]
        first_binary_tlsh = first_binary_data.tlsh
        resp = self.api.binaries_meta.get_similar_tlsh_entities(tlsh=first_binary_tlsh)
        self.assertGreater(len(resp.matches), 1)

    def test_get_similar_entities(self):
        first = self.sha256s[0]
        similar_entities = self.api.binaries_meta.get_similar_entities(first)
        self.assertIsInstance(similar_entities, models_restapi.SimilarMatch)

    def test_get_nearby_entities(self):
        first = self.sha256s[0]
        nearby_resp = self.api.binaries_meta.get_nearby_entities(first)
        self.assertIsInstance(nearby_resp, models_restapi.ReadNearby)
        self.assertEqual(nearby_resp.id_focus, first)
        # There should at least be 1 links (the node)
        self.assertGreaterEqual(len(nearby_resp.links), 1)

        nearby_resp = self.api.binaries_meta.get_nearby_entities(
            first, include_cousins=models_restapi.IncludeCousinsEnum.Large
        )
        self.assertIsInstance(nearby_resp, models_restapi.ReadNearby)
        self.assertEqual(nearby_resp.id_focus, first)
        # There should at least be 1 links (the node)
        self.assertGreaterEqual(len(nearby_resp.links), 1)

        nearby_resp = self.api.binaries_meta.get_nearby_entities(
            first, include_cousins=models_restapi.IncludeCousinsEnum.No
        )
        self.assertIsInstance(nearby_resp, models_restapi.ReadNearby)
        self.assertEqual(nearby_resp.id_focus, first)
        # There should at least be 1 links (the node)
        self.assertGreaterEqual(len(nearby_resp.links), 1)

        nearby_resp = self.api.binaries_meta.get_nearby_entities(self.non_existent_sha256)
        # No links should be present on non-existing binary.
        self.assertEqual(nearby_resp.id_focus, self.non_existent_sha256)
        self.assertEqual(len(nearby_resp.links), 0)

    def test_get_binary_tags(self):
        """Test getting tags works (also tested in create and delete tag methods)."""
        first = self.sha256s[0]
        tags = self.api.binaries_meta.get_binary_tags(first)
        self.assertIsInstance(tags, models_restapi.ReadAllEntityTags)

        # Non-existent hash has nothing to get.
        resp = self.api.binaries_meta.get_binary_tags(self.non_existent_sha256)
        self.assertEqual(len(resp.items), 0)

    def test_create_and_delete_tag_on_binary(self):
        """Tests a tag can be seen with get and then creates and deletes the tag.

        If the tag already exists, it is first deleted and then created.
        """
        # Maximum number of times to recheck a tag exists before giving up.
        max_check_retry_count = 5

        def check_tag_exists(sha256: str) -> bool:
            """Check a tag exists on a binary."""
            found_tags = self.api.binaries_meta.get_binary_tags(sha256)
            for cur_tag in found_tags.items:
                if cur_tag.tag == self.tag_to_create:
                    return True
            return False

        def verify_delete(sha256: str):
            """Delete a tag and verify it's no longer present on the binary."""
            self.api.binaries_meta.delete_tag_on_binary(sha256, self.tag_to_create)
            is_tag_exists = check_tag_exists(sha256)
            retry_count = 0
            while is_tag_exists and retry_count < max_check_retry_count:
                retry_count += 1
                time.sleep(1)
                is_tag_exists = check_tag_exists(sha256)
            return is_tag_exists

        def verify_create(sha256: str):
            """Create a tag and verify it's on the binary."""
            self.api.binaries_meta.create_tag_on_binary(sha256, self.tag_to_create, self.default_security)
            is_tag_exists = check_tag_exists(sha256)
            retry_count = 0
            while not is_tag_exists and retry_count < max_check_retry_count:
                retry_count += 1
                time.sleep(1)
                is_tag_exists = check_tag_exists(sha256)
            return is_tag_exists

        first = self.sha256s[0]
        # Check if tag is already present and then delete or create the tag first accordingly.
        is_tag_already_present = check_tag_exists(first)

        # Iteration 1 delete or create.
        if is_tag_already_present:
            self.assertFalse(verify_delete(first))
        else:
            self.assertTrue(verify_create(first))

        # Iteration 2 delete or create.
        if is_tag_already_present:
            self.assertTrue(verify_create(first))
        else:
            self.assertFalse(verify_delete(first))

        # Create on non existent sha256 (no error nothing happens)
        self.api.binaries_meta.create_tag_on_binary(
            self.non_existent_sha256, self.tag_to_create, self.default_security
        )
        # Delete on non existent sha256 (no error nothing happens)
        self.api.binaries_meta.delete_tag_on_binary(self.non_existent_sha256, self.tag_to_create)

    def test_get_binary_status(self):
        # Verify some plugins have status events.
        first = self.sha256s[0]
        binary_statuses = self.api.binaries_meta.get_binary_status(first)
        self.assertGreater(len(binary_statuses.items), 1)
        self.assertIsInstance(binary_statuses.items[0], models_restapi.StatusEvent)

        # No events if it doesn't exist.
        with self.assertRaises(BadResponse404):
            binary_statuses = self.api.binaries_meta.get_binary_status(self.non_existent_sha256)

    def test_get_binary_documents(self):
        # Standard loading of documents.
        first = self.sha256s[0]
        opensearch_docs = self.api.binaries_meta.get_binary_documents(first)
        self.assertGreaterEqual(len(opensearch_docs.items), 1)
        # Find at least one sourced event
        opensearch_docs = self.api.binaries_meta.get_binary_documents(first, action=azm.BinaryAction.Sourced, size=200)
        self.assertGreaterEqual(len(opensearch_docs.items), 1)

        # Non existent binary shouldn't have any sourced events.
        with self.assertRaises(BadResponse404):  # Expecting 404
            self.api.binaries_meta.get_binary_documents(self.non_existent_sha256, action=azm.BinaryAction.Sourced)
