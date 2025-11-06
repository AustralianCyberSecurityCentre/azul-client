import json

from azul_bedrock import models_restapi

from azul_client.exceptions import BadResponse, BadResponse404

from .base_test import BaseApiTest


class TestSourcesApi(BaseApiTest):
    @classmethod
    def setUpClass(cls):
        cls.common_feature_magic = "magic"
        cls.common_feature_file_extension = "file_extension"
        cls.entropy_feature = "entropy"
        # There should be entropy of 0 in the system somewhere
        cls.known_magic_value = "ASCII text, with CRLF line terminators"
        cls.known_entropy_value = "0"
        cls.known_extension_feat_val = "txt"
        # Minimum number of values you expected a large feature like the above three to have.
        cls.large_feature_minimum_values = 10
        cls.entropy_xpart = "integer"

        # tagging
        cls.feature_tag_entropy = "test-fv-tag-1"
        cls.feature_tag_extension = "test-fv-tag-2"
        cls.feature_tag_magic = "test-fv-tag-3"
        super().setUpClass()

        # Create two feature_value tags (Note this is tested in this class but also needed to test this class.fd)
        tag_created_1 = cls.api.features.create_feature_value_tag(
            tag=cls.feature_tag_entropy,
            feature=cls.entropy_feature,
            value=cls.known_entropy_value,
            security=cls.default_security,
        )
        assert tag_created_1 is None, "Failed to setup for feature test, does Entropy have a single 0 value?"
        tag_created_2 = cls.api.features.create_feature_value_tag(
            tag=cls.feature_tag_extension,
            feature=cls.common_feature_file_extension,
            value=cls.known_extension_feat_val,
            security=cls.default_security,
        )
        assert (
            tag_created_2 is None
        ), "Failed to setup for feature test, is there at least one file with a txt extension."

        # Find the current version of Entropy for later use.
        list_of_plugins = cls.api.plugins.get_all_plugins()
        current_version_of_entropy = None
        for plugin in list_of_plugins:
            if plugin.newest_version.name == cls.entropy_plugin_name:
                current_version_of_entropy = plugin.newest_version.version
                break
        if current_version_of_entropy is None:
            raise Exception(
                "Something is wrong with the plugins API or Entropy, can't find latest version of entropy."
            )
        cls.entropy_version = current_version_of_entropy

    def test_count_unique_values_in_feature(self):
        """Test counting the number of unique feature/value combinations there are for a given feature."""
        empty_counts = self.api.features.count_unique_values_in_feature([])
        self.assertEqual(empty_counts, dict())

        # MAGIC FEATURE COUNT ---
        magic_count = self.api.features.count_unique_values_in_feature([self.common_feature_magic])
        self.assertEqual(len(magic_count.keys()), 1)
        self.assertIn(self.common_feature_magic, magic_count.keys())
        count_val = magic_count[self.common_feature_magic]
        self.assertEqual(count_val.name, self.common_feature_magic)
        # There should be at least 20 magic values as there will be hundreds or thousands on a large system.
        self.assertGreaterEqual(count_val.values, self.large_feature_minimum_values)

        # MAGIC AND FILE EXTENSION FEATURE COUNT ---
        multi_count = self.api.features.count_unique_values_in_feature(
            [self.common_feature_magic, self.common_feature_file_extension]
        )
        self.assertGreaterEqual(len(multi_count.keys()), 2)
        self.assertIn(self.common_feature_magic, multi_count.keys())
        self.assertIn(self.common_feature_file_extension, multi_count.keys())

        # ENTROPY with just author name ---
        entropy_response = self.api.features.count_unique_values_in_feature(
            [self.entropy_feature], author=self.entropy_plugin_name
        )
        self.assertEqual(list(entropy_response.keys()), [self.entropy_feature])
        # Should be at least 20 entropies calculated ---
        self.assertGreaterEqual(entropy_response[self.entropy_feature].values, self.large_feature_minimum_values)

        # ENTROPY with author name and version ---
        entropy_response_2 = self.api.features.count_unique_values_in_feature(
            [self.entropy_feature], author=self.entropy_plugin_name, author_version=self.entropy_version
        )
        self.assertEqual(list(entropy_response_2.keys()), [self.entropy_feature])
        # Should be at least 20 entropies calculated
        self.assertGreaterEqual(entropy_response_2[self.entropy_feature].values, self.large_feature_minimum_values)

    def test_count_unique_entities_in_features(self):
        """Test counting the number of entities(binaries) that have a particular feature."""
        empty_counts = self.api.features.count_unique_values_in_feature([])
        self.assertEqual(empty_counts, dict())

        # MAGIC FEATURE COUNT ---
        magic_count = self.api.features.count_unique_entities_in_features([self.common_feature_magic])
        self.assertEqual(len(magic_count.keys()), 1)
        self.assertIn(self.common_feature_magic, magic_count.keys())
        count_val = magic_count[self.common_feature_magic]
        self.assertEqual(count_val.name, self.common_feature_magic)
        # There should be at least 20 entities with magic as there will be hundreds or thousands on a large system.
        self.assertGreaterEqual(count_val.entities, self.large_feature_minimum_values)

        # MAGIC AND FILE EXTENSION FEATURE COUNT ---
        multi_count = self.api.features.count_unique_entities_in_features(
            [self.common_feature_magic, self.common_feature_file_extension]
        )
        self.assertGreaterEqual(len(multi_count.keys()), 2)
        self.assertIn(self.common_feature_magic, multi_count.keys())
        self.assertIn(self.common_feature_file_extension, multi_count.keys())

        # ENTROPY with just author name ---
        entropy_response = self.api.features.count_unique_entities_in_features(
            [self.entropy_feature], author=self.entropy_plugin_name
        )
        self.assertEqual(list(entropy_response.keys()), [self.entropy_feature])
        # Should be at least 20 entropies calculated ---
        self.assertGreaterEqual(entropy_response[self.entropy_feature].entities, self.large_feature_minimum_values)

        # ENTROPY with author name and version ---
        entropy_response_2 = self.api.features.count_unique_entities_in_features(
            [self.entropy_feature], author=self.entropy_plugin_name, author_version=self.entropy_version
        )
        self.assertEqual(list(entropy_response_2.keys()), [self.entropy_feature])
        # Should be at least 20 entropies calculated
        self.assertGreaterEqual(entropy_response_2[self.entropy_feature].entities, self.large_feature_minimum_values)

    def test_count_unique_entities_in_featurevalues(self):
        """Test counting the number of entities(binaries) that have a particular feature/value pair."""
        resp = self.api.features.count_unique_entities_in_featurevalues([])
        self.assertEqual(resp, dict())

        # Request with feature and value no AUTHOR ---
        resp = self.api.features.count_unique_entities_in_featurevalues(
            [models_restapi.ValueCountItem(name=self.entropy_feature, value=self.known_entropy_value)]
        )
        self.assertEqual(list(resp.keys()), [self.entropy_feature])
        self.assertEqual(list(resp[self.entropy_feature].keys())[0], self.known_entropy_value)
        val_entropy_feature_count = resp[self.entropy_feature][self.known_entropy_value]
        self.assertEqual(val_entropy_feature_count.value, self.known_entropy_value)
        self.assertGreater(val_entropy_feature_count.entities, self.large_feature_minimum_values)

        # Request with feature and value with AUTHOR ---
        resp = self.api.features.count_unique_entities_in_featurevalues(
            [models_restapi.ValueCountItem(name=self.entropy_feature, value=self.known_entropy_value)],
            author=self.entropy_plugin_name,
        )
        self.assertEqual(list(resp.keys()), [self.entropy_feature])
        self.assertEqual(list(resp[self.entropy_feature].keys())[0], self.known_entropy_value)
        val_entropy_feature_count = resp[self.entropy_feature][self.known_entropy_value]
        self.assertEqual(val_entropy_feature_count.value, self.known_entropy_value)
        self.assertGreater(val_entropy_feature_count.entities, self.large_feature_minimum_values)

        # Request with feature and value with AUTHOR AND VERSION ---
        resp = self.api.features.count_unique_entities_in_featurevalues(
            [models_restapi.ValueCountItem(name=self.entropy_feature, value=self.known_entropy_value)],
            author=self.entropy_plugin_name,
            author_version=self.entropy_version,
        )
        self.assertEqual(list(resp.keys()), [self.entropy_feature])
        self.assertEqual(list(resp[self.entropy_feature].keys())[0], self.known_entropy_value)
        val_entropy_feature_count = resp[self.entropy_feature][self.known_entropy_value]
        self.assertEqual(val_entropy_feature_count.value, self.known_entropy_value)
        self.assertGreater(val_entropy_feature_count.entities, self.large_feature_minimum_values)

    def test_count_unique_entities_in_featurevalueparts(self):
        """Test the part counts of number of binaries/entities with a given feature value."""
        resp = self.api.features.count_unique_entities_in_featurevalues([])
        self.assertEqual(resp, dict())

        # Request with feature and value no AUTHOR ---
        # expected form {'0': {'integer': models.ValuePartCountRet(value='0', part='integer', entities=3025848)}}
        resp = self.api.features.count_unique_entities_in_featurevalueparts(
            [models_restapi.ValuePartCountItem(part=self.entropy_xpart, value=self.known_entropy_value)]
        )
        self.assertEqual(list(resp.keys()), [self.known_entropy_value])
        found_value = resp[self.known_entropy_value]
        self.assertEqual(list(found_value.keys()), [self.entropy_xpart])
        self.assertEqual(found_value[self.entropy_xpart].value, self.known_entropy_value)
        self.assertGreater(found_value[self.entropy_xpart].entities, self.large_feature_minimum_values)

        # Request with feature and value with AUTHOR ---
        resp = self.api.features.count_unique_entities_in_featurevalueparts(
            [models_restapi.ValuePartCountItem(part=self.entropy_xpart, value=self.known_entropy_value)],
            author=self.entropy_plugin_name,
        )
        self.assertEqual(list(resp.keys()), [self.known_entropy_value])
        found_value = resp[self.known_entropy_value]
        self.assertEqual(list(found_value.keys()), [self.entropy_xpart])
        self.assertEqual(found_value[self.entropy_xpart].value, self.known_entropy_value)
        self.assertEqual(found_value[self.entropy_xpart].entities, 0)

        # Request with feature and value with AUTHOR AND VERSION ---
        resp = self.api.features.count_unique_entities_in_featurevalueparts(
            [models_restapi.ValuePartCountItem(part=self.entropy_xpart, value=self.known_entropy_value)],
            author=self.entropy_plugin_name,
            author_version=self.entropy_version,
        )
        self.assertEqual(list(resp.keys()), [self.known_entropy_value])
        found_value = resp[self.known_entropy_value]
        self.assertEqual(list(found_value.keys()), [self.entropy_xpart])
        self.assertEqual(found_value[self.entropy_xpart].value, self.known_entropy_value)
        self.assertEqual(found_value[self.entropy_xpart].entities, 0)

    def test_get_all_feature_value_tags(self):
        resp = self.api.features.get_all_feature_value_tags()
        self.assertGreaterEqual(resp.num_tags, 1)
        self.assertEqual(len(resp.tags), resp.num_tags)

    def test_get_feature_values_in_tag(self):
        """Test getting a list of all features and values for a given tag."""
        resp = self.api.features.get_feature_values_in_tag(self.feature_tag_entropy)
        is_entropy_fv_found = False
        for feature_val_tag in resp.items:
            # Verify we've found the expected feature/value tagged.
            if (
                feature_val_tag.feature_name == self.entropy_feature
                and feature_val_tag.feature_value == self.known_entropy_value
            ):
                is_entropy_fv_found = True

        self.assertTrue(
            is_entropy_fv_found,
            f"Entropy's known value '{self.known_entropy_value}' wasn't found with the appropriate tag,"
            + f" fv with tag were {resp.items}",
        )

    def test_create_feature_value_tag(self):
        resp = self.api.features.create_feature_value_tag(
            self.feature_tag_magic, self.common_feature_magic, self.known_magic_value, self.default_security
        )
        self.assertIsNone(resp)

        # Creation with bad security.
        with self.assertRaises(BadResponse):
            self.api.features.create_feature_value_tag(
                self.feature_tag_magic, self.common_feature_magic, self.known_magic_value, "SJDKFNIEJUNIJNSEJCDN D"
            )

    def test_delete_feature_value_tag(self):
        def _verify_feature_value():
            found = False
            resp = self.api.features.get_feature_values_in_tag(self.feature_tag_extension)
            for fv in resp.items:
                if (
                    fv.feature_name == self.common_feature_file_extension
                    and fv.feature_value == self.known_extension_feat_val
                ):
                    found = True
                    break
            return found

        self.assertTrue(
            _verify_feature_value(),
            f"Feature '{self.common_feature_file_extension}' with expected value "
            + f"'{self.known_extension_feat_val}' does not exist to delete.",
        )
        self.api.features.delete_feature_value_tag(
            self.feature_tag_extension, self.common_feature_file_extension, self.known_extension_feat_val
        )
        self.assertFalse(
            _verify_feature_value(),
            f"Feature '{self.common_feature_file_extension}' with expected value "
            + f"'{self.known_extension_feat_val}' was not deleted.",
        )

    def test_find_features(self):
        resp = self.api.features.find_features()
        self.assertGreater(len(resp.items), self.large_feature_minimum_values)
        resp = self.api.features.find_features(author=self.entropy_plugin_name)
        self.assertGreaterEqual(len(resp.items), 1)
        resp = self.api.features.find_features(author=self.entropy_plugin_name, author_version=self.entropy_version)
        self.assertGreaterEqual(len(resp.items), 1)
        self.assertIn(self.entropy_feature, [fv.name for fv in resp.items])
        # No items as author is invalid.
        resp = self.api.features.find_features(author="jakldfjaklvnewiourjnsd")
        self.assertEqual(len(resp.items), 0)

    def test_find_values_in_feature_minimal(self):
        resp = self.api.features.find_values_in_feature(self.entropy_feature)
        self.assertEqual(resp.name, self.entropy_feature)
        self.assertEqual(resp.type, "float")
        self.assertGreater(len(resp.values), self.large_feature_minimum_values)
        self.assertIsNotNone(resp.after)

    def test_find_values_in_feature_with_term(self):
        """Search for the start of the known feature value and it should be found in the term query."""
        resp = self.api.features.find_values_in_feature(
            self.common_feature_file_extension, term=self.known_extension_feat_val[0]
        )
        self.assertEqual(resp.name, self.common_feature_file_extension)
        self.assertEqual(resp.type, "string")
        self.assertGreaterEqual(len(resp.values), 1)
        self.assertGreaterEqual(resp.total, 1)

        self.assertIn(self.known_extension_feat_val, [val.value for val in resp.values])

    def test_find_values_in_feature_with_author_version(self):
        """Search with author and sorts set."""
        resp = self.api.features.find_values_in_feature(
            self.entropy_feature, sort_asc=False, case_insensitive=True, author_version=self.entropy_version
        )
        self.assertEqual(resp.name, self.entropy_feature)
        self.assertEqual(resp.type, "float")
        self.assertGreater(len(resp.values), self.large_feature_minimum_values)
        self.assertGreater(len(resp.after if resp.after else ""), 1)
