import copy
import unittest

import pendulum
from azul_bedrock import models_restapi

from azul_client.api import Api
from azul_client.api.binaries_meta import FindOptions
from azul_client.config import get_config

from . import module_ref, module_sha256s, module_source


class TestBinaryFind(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.non_existent_sha256 = "ed7002b439e9ac845f22357d822bac1444730fbdb6016d3ec9432297b9ec9f74"
        cls.api = Api(get_config())
        cls.source = module_source
        cls.ref = module_ref
        cls.sha256s = copy.copy(module_sha256s)
        cls.default_min_hash_count = 10
        cls.source1 = "virustotal"
        cls.source2 = "watch"
        cls.common_plugin_name = "Entropy"
        cls.common_feature_value = "txt"
        cls.common_feature_key = "malformed"
        cls.common_file_format_legacy = "Text"
        cls.common_file_format = "text/plain"
        cls.dummy_tag = "dummy"
        cls.user = module_ref.get("user")

    # -------------------------------- Hash Query

    def test_find_hashes(self):
        result = self.api.binaries_meta.find_hashes([self.sha256s[0]])
        self.assertEqual(1, len(result.items))
        self.assertEqual(self.sha256s[0], result.items[0].sha256)
        self.assertTrue(result.items[0].exists)

        sub_sha256s = self.sha256s[0:5]
        result = self.api.binaries_meta.find_hashes(sub_sha256s)
        self.assertEqual(5, len(result.items))
        # Verify that all of the sha256's found are equal to the ones in the list.
        found_hashes = set()
        for item in result.items:
            self.assertIn(item.sha256, sub_sha256s)
            found_hashes.add(item.sha256)
        # Ensure all found hashes are unique.
        self.assertEqual(5, len(found_hashes))

        result = self.api.binaries_meta.find_hashes([self.non_existent_sha256])
        self.assertEqual(1, len(result.items))
        self.assertFalse(result.items[0].exists)

    # -------------------------------- Term Query
    def test_find_term_query_basic(self):
        term = f'source.encoded_references.value:"{self.user}"'
        resp = self.api.binaries_meta.find(term)
        # Check in the module setup if this number needs to be increased/decreased.
        self.assertGreaterEqual(len(resp.items), self.default_min_hash_count)

    def _assert_sorting_source_timestamp(
        self, resp_newest_first: models_restapi.EntityFind, resp_oldest_first: models_restapi.EntityFind
    ):
        """Verifies that source_timestamp filtering is working appropriately.

        First checks at least one of the newest submissions are one of the ones subitted by the test setup.
        Then checks that the first item in the newest list has a larger source date than the oldest list.
        """
        # if sorting worked the reverse of one list equals the other
        resp_newest_first_sha256s = list([i.sha256 for i in resp_newest_first.items])

        # Verify at least one of the newest
        at_least_one_hash_found = False
        for newest_hash in resp_newest_first_sha256s:
            if newest_hash in self.sha256s:
                at_least_one_hash_found = True
        self.assertTrue(
            at_least_one_hash_found,
            "None of the newest submission hashes were found when sorting by "
            + f"newest submission first and these should be the newest submissions! [{", ".join(resp_newest_first_sha256s)}]",
        )
        newest = pendulum.parse(resp_newest_first.items[0].sources[0].timestamp)
        oldest = pendulum.parse(resp_oldest_first.items[0].sources[0].timestamp)
        self.assertGreaterEqual(newest, oldest)

    def test_find_term_query_sorting(self):
        term = f'source.encoded_references.value:"{self.user}"'
        resp_oldest_first = self.api.binaries_meta.find(
            term, sort_asc=True, sort_prop=models_restapi.FindBinariesSortEnum.source_timestamp
        )
        resp_newest_first = self.api.binaries_meta.find(
            term, sort_asc=False, sort_prop=models_restapi.FindBinariesSortEnum.source_timestamp
        )
        self._assert_sorting_source_timestamp(resp_newest_first, resp_oldest_first)

    def test_find_term_query_limiting(self):
        term = f'source.encoded_references.value:"{self.user}"'
        resp = self.api.binaries_meta.find(term, max_entities=2)
        # Check in the module setup if this number needs to be increased/decreased.
        self.assertGreaterEqual(len(resp.items), 2)

    def test_find_term_query_count(self):
        term = f'source.encoded_references.value:"{self.user}"'
        resp = self.api.binaries_meta.find(term, count_entities=True)
        self.assertIsNotNone(resp.items_count)

    # -------------------------------- Simple Query
    def test_find_simple_basic(self):
        find_options = FindOptions(source_username=self.user)
        resp = self.api.binaries_meta.find_simple(find_options)
        # Check in the module setup if this number needs to be increased/decreased.
        self.assertGreaterEqual(len(resp.items), self.default_min_hash_count)

    def test_find_simple_sorting(self):
        find_options = FindOptions(source_username=self.user)
        resp_oldest_first = self.api.binaries_meta.find_simple(
            find_options, sort_asc=True, sort_prop=models_restapi.FindBinariesSortEnum.source_timestamp
        )
        resp_newest_first = self.api.binaries_meta.find_simple(
            find_options, sort_asc=False, sort_prop=models_restapi.FindBinariesSortEnum.source_timestamp
        )
        self._assert_sorting_source_timestamp(resp_newest_first, resp_oldest_first)

    def test_find_simple_limit(self):
        find_options = FindOptions(source_username=self.user)
        resp = self.api.binaries_meta.find_simple(find_options, max_entities=2)
        # Check in the module setup if this number needs to be increased/decreased.
        self.assertGreaterEqual(len(resp.items), 2)

    def test_find_simple_count(self):
        find_options = FindOptions(source_username=self.user)
        resp = self.api.binaries_meta.find_simple(find_options, count_entities=True)
        self.assertIsNotNone(resp.items_count)

    def test_simple_compound_query(self):
        options = FindOptions(
            source_username=self.user,
            sources=self.source,
            source_timestamp_older=pendulum.now(),
            source_timestamp_newer=pendulum.datetime(2020, 1, 1, 1, 1, 1, 1, 1),
            source_depth=0,
        )
        self._search_count_ge(options, self.default_min_hash_count)

    # -------------------------------- Simple Query - extended
    def _check_doesnt_fail(self, options: FindOptions) -> models_restapi.EntityFind:
        self._search_count_ge(options, 0)

    # --- SOURCE ID
    def _search_count_ge(self, options: FindOptions, count: int) -> models_restapi.EntityFind:
        resp = self.api.binaries_meta.find_simple(options, max_entities=count + 1)
        self.assertGreaterEqual(len(resp.items), count)
        return resp

    def _search_count_equal(self, options: FindOptions, count: int):
        # Look for just enough values to fail.
        resp = self.api.binaries_meta.find_simple(options, max_entities=count + 1)
        self.assertEqual(len(resp.items), count)

    def _get_source_ids(self, resp: models_restapi.EntityFind) -> set[str]:
        source_list = set()
        for val in resp.items:
            if not val.sources:
                # Note - This shouldn't be needed as sources should always be populated, but sometimes they aren't.
                continue
            for src in val.sources:
                source_list.add(src.name)
        return source_list

    def test_find_simple_sources(self):
        # single
        find_options = FindOptions(sources=self.source)
        resp = self._search_count_ge(find_options, self.default_min_hash_count)

        self.assertIn(self.source, self._get_source_ids(resp))
        # multiple
        included_sources = [self.source, self.source1]
        find_options = FindOptions(sources=included_sources)
        resp2 = self._search_count_ge(find_options, self.default_min_hash_count)
        at_least_one_source_included = False
        for src in self._get_source_ids(resp2):
            if src in included_sources:
                at_least_one_source_included = True
        self.assertTrue(at_least_one_source_included)

    def test_find_simple_sources_exclude(self):
        # single
        find_options = FindOptions(source_excludes=self.source1)
        resp = self._search_count_ge(find_options, self.default_min_hash_count)
        self.assertNotIn(self.source1, self._get_source_ids(resp))

        # multiple
        excluded_sources = [self.source1, self.source2]
        find_options = FindOptions(source_excludes=excluded_sources)
        resp2 = self._search_count_ge(find_options, self.default_min_hash_count)
        for src in self._get_source_ids(resp2):
            self.assertNotIn(src, excluded_sources)

    # --- SOURCE DEPTH

    def _get_source_depths(self, resp: models_restapi.EntityFind, prefer_depths: list[int] = None) -> list[int]:
        depths = []
        for item in resp.items:
            if not item.sources:
                # Note - This shouldn't be needed as sources should always be populated.
                continue
            current_item_depths = []
            current_item_depths_filtered = []
            for src in item.sources:
                if src:  # Ignore ids with no sources
                    current_item_depths.append(src.depth)
                    # Only keep the depths we expect
                    if prefer_depths and src.depth in prefer_depths:
                        print(f"Preferring {src.depth}")
                        current_item_depths_filtered.append(src.depth)
            # If the filtered response has data we have the depths we expected and maybe more.
            if len(current_item_depths_filtered) > 0:
                depths += current_item_depths_filtered
            else:
                # filter has filtered out all results so go with the unfiltered result.
                depths += current_item_depths
                print(f"(probably an error) accepting non preferred with values {current_item_depths} {item.sha256}")
        return depths

    # Depth is very hard to filter by, because Assemblyline meta-data is mapped which has a source_depth of 1.
    # But isn't the preferred source event.
    # When searching for an event with depth greater than 0 Assemblyline events that have been mapped will have depth 1
    # So those binaries will be 'valid' however when the binary is selected the actual depth provided will be 0
    # Because that's the depth of the source event which makes it hard to verify anything is working.

    # def test_find_simple_sources_source_depth(self):
    #     find_options = FindOptions(source_depth=0)
    #     resp = self._search_count_ge(find_options, self.default_min_hash_count)
    #     self.assertIn(0, self._get_source_depths(resp, prefer_depths=[0]))

    #     included_depths = [1, 2, 3, 4, 5, 6]
    #     find_options = FindOptions(source_depth=included_depths)
    #     resp2 = self._search_count_ge(find_options, self.default_min_hash_count)
    #     for d in self._get_source_depths(resp2, prefer_depths=included_depths):
    #         self.assertIn(d, included_depths)

    # FUTURE - the filter technically works but there is no way to verify the result.
    # Because metastore prefers the numerically smallest depth if you look for depth > 1 and there is depth
    # 1,2,3 on a binary it will be found and show depth 1 because that is the smallest number.
    # def test_find_simple_sources_source_depth_exclude(self):
    #     find_options = FindOptions(source_depth_exclude=1)
    #     resp = self._search_count_ge(find_options, 1)
    #     self.assertNotIn(1, self._get_source_depths(resp, prefer_depths=[2, 3, 4, 5, 6, 7, 8, 9, 10]))

    #     excluded_depths = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    #     find_options = FindOptions(source_depth_exclude=excluded_depths)
    #     resp = self._search_count_ge(find_options, self.default_min_hash_count)
    #     for d in self._get_source_depths(resp, prefer_depths=[0, 10]):
    #         self.assertNotIn(d, excluded_depths)

    # def test_find_simple_sources_source_depth_less_than(self):
    #     find_options = FindOptions(source_depth_less=1)
    #     resp = self.api.binaries_meta.find_simple(find_options, max_entities=self.default_min_hash_count)
    #     for d in self._get_source_depths(resp, prefer_depths=[0]):
    #         self.assertLess(d, 1)

    # def test_find_simple_sources_source_depth_greater(self):
    #     find_options = FindOptions(source_depth_greater=0)
    #     resp = self._search_count_ge(find_options, self.default_min_hash_count)
    #     for d in self._get_source_depths(resp, prefer_depths=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]):
    #         self.assertGreater(d, 0)

    # --- SOURCE TIMESTAMP
    def _check_timestamp(self, find_options: FindOptions, newer: bool, potentially_equal: bool):
        self._search_count_ge(find_options, self.default_min_hash_count)

    def test_find_entity_newer_than(self):
        find_options = FindOptions(source_timestamp_newer=pendulum.datetime(2020, 1, 1, 1, 1, 1, 1, 1))
        self._search_count_ge(find_options, self.default_min_hash_count)

    def test_find_entity_newer_than_or_equal_to(self):
        find_options = FindOptions(source_timestamp_newer_or_equal=pendulum.datetime(2020, 1, 1, 1, 1, 1, 1, 1))
        self._search_count_ge(find_options, self.default_min_hash_count)

    def test_find_entity_older_than(self):
        find_options = FindOptions(source_timestamp_older=pendulum.now())
        self._search_count_ge(find_options, self.default_min_hash_count)

    def test_find_entity_older_than_or_equal_to(self):
        find_options = FindOptions(source_timestamp_older_or_equal=pendulum.now())
        self._search_count_ge(find_options, self.default_min_hash_count)

    # --- PLUGIN/AUTHOR
    def test_find_entity_with_features_from_plugin_name(self):
        find_options = FindOptions(plugin_name=self.common_plugin_name)
        self._search_count_ge(find_options, self.default_min_hash_count)

    def test_find_entity_with_features_from_plugin_version(self):
        find_options = FindOptions(plugin_version="1.1.1")
        # Any 200 response is acceptable because, there may be no plugin version 1.1.1.
        self._check_doesnt_fail(find_options)

    # --- FEATURES
    def test_find_entity_with_feature_name(self):
        find_options = FindOptions(has_feature_keys=self.common_feature_key)
        self._search_count_ge(find_options, self.default_min_hash_count)

    def test_find_entity_with_feature_value(self):
        find_options = FindOptions(has_feature_values=self.common_feature_value)
        self._search_count_ge(find_options, self.default_min_hash_count)

    # --- BINARY INFO - ENTITY SIZE
    def test_find_entity_size_greater(self):
        find_options = FindOptions(greater_than_size_bytes=10)
        self._search_count_ge(find_options, self.default_min_hash_count)

    def test_find_entity_size_smaller(self):
        find_options = FindOptions(less_than_size_bytes=20000)
        self._search_count_ge(find_options, self.default_min_hash_count)

    # --- BINARY INFO - FILE TYPE
    def test_find_simple_file_format_legacy(self):
        # Normal
        find_options = FindOptions(file_formats_legacy=self.common_file_format_legacy)
        resp = self._search_count_ge(find_options, self.default_min_hash_count)
        for item in resp.items:
            self.assertEqual(self.common_file_format_legacy, item.file_format_legacy)
        # List
        find_options = FindOptions(file_formats_legacy=[self.common_file_format_legacy])
        resp = self._search_count_ge(find_options, self.default_min_hash_count)
        for item in resp.items:
            self.assertEqual(self.common_file_format_legacy, item.file_format_legacy)

    def test_find_simple_file_format_legacy_exclude(self):
        # Normal
        find_options = FindOptions(file_formats_legacy_exclude=self.common_file_format_legacy)
        resp = self._search_count_ge(find_options, self.default_min_hash_count)
        for item in resp.items:
            self.assertNotEqual(self.common_file_format_legacy, item.file_format_legacy)
        # List
        find_options = FindOptions(file_formats_legacy_exclude=[self.common_file_format_legacy])
        resp = self._search_count_ge(find_options, self.default_min_hash_count)
        for item in resp.items:
            self.assertNotEqual(self.common_file_format_legacy, item.file_format_legacy)

    def test_find_simple_file_format(self):
        # Normal
        find_options = FindOptions(file_formats=self.common_file_format)
        resp = self._search_count_ge(find_options, self.default_min_hash_count)
        for item in resp.items:
            self.assertEqual(self.common_file_format, item.file_format)
        # List
        find_options = FindOptions(file_formats=[self.common_file_format])
        resp = self._search_count_ge(find_options, self.default_min_hash_count)
        for item in resp.items:
            self.assertEqual(self.common_file_format, item.file_format)

    def test_find_simple_file_format_exclude(self):
        # Normal
        find_options = FindOptions(file_formats_exclude=self.common_file_format)
        resp = self._search_count_ge(find_options, self.default_min_hash_count)
        for item in resp.items:
            self.assertNotEqual(self.common_file_format, item.file_format)
        # List
        find_options = FindOptions(file_formats_exclude=[self.common_file_format])
        resp = self._search_count_ge(find_options, self.default_min_hash_count)
        for item in resp.items:
            self.assertNotEqual(self.common_file_format, item.file_format)

    def test_find_all(self):
        # should have at least 400 binaries of any kind
        found = 0
        find_options = FindOptions()
        find_result = self.api.binaries_meta.find_all(find_options, request_binaries=100, max_binaries=400)
        for item in find_result.iter:
            self.assertGreaterEqual(find_result.approx_total, 400)
            self.assertIsNotNone(item)
            found += 1
            if found > 400:
                break
        self.assertGreaterEqual(found, 400)

        # should have at least 40 binaries of common kind
        found = 0
        find_options = FindOptions(file_formats_exclude=self.common_file_format)
        find_result = self.api.binaries_meta.find_all(find_options, request_binaries=100, max_binaries=400)
        for item in find_result.iter:
            self.assertGreaterEqual(find_result.approx_total, 40)
            self.assertIsNotNone(item)
            found += 1
            if found > 40:
                break
        self.assertGreaterEqual(found, 40)

        # Should still be fine if there are no binaries that meet condition
        find_result = self.api.binaries_meta.find_all(
            FindOptions(greater_than_size_bytes=1000000000000000), request_binaries=100, max_binaries=1
        )
        for item in find_result:
            self.assertTrue(False, "Shouldn't iterate over a length zero iterator")
        self.assertEqual(find_result.approx_total, 0)

    # --- TAGS
    # FUTURE - can't test this search functionality because we need to be able to add a tag to a binary/feature to test the filter.
    # def test_find_simple_binary_tag(self):
    #     # At least a 200
    #     find_options = FindOptions(binary_tags=self.dummy_tag)
    #     self._check_doesnt_fail(find_options)

    # def test_find_simple_feature_tag(self):
    #     # At least a 200
    #     find_options = FindOptions(feature_tags=self.dummy_tag)
    #     self._check_doesnt_fail(find_options)
