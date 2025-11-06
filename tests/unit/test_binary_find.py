from unittest import TestCase

from azul_client.api.binaries_meta import FindOptions


class BinaryFindUnitTest(TestCase):
    def test_find_with_zero_integer_cases(self):
        """Regression test to ensure setting an int value to 0 still includes the filter."""
        find_options = FindOptions(source_depth=[0])
        self.assertEqual("depth:0", find_options.to_query())

        find_options = FindOptions(source_depth=0)
        self.assertEqual("depth:0", find_options.to_query())

        find_options = FindOptions(source_depth=None)
        self.assertEqual("", find_options.to_query())

        find_options = FindOptions(source_depth=[])
        self.assertEqual("", find_options.to_query())

        find_options = FindOptions(greater_than_size_bytes=0)
        self.assertEqual("size:>0", find_options.to_query())

        find_options = FindOptions(greater_than_size_bytes=None)
        self.assertEqual("", find_options.to_query())

    def test_find_with_empty_string(self):
        """Regression test to allow for empty strings to be set for values."""
        find_options = FindOptions(sources="")
        self.assertEqual('source.name:""', find_options.to_query())

        find_options = FindOptions(sources=[""])
        self.assertEqual('source.name:""', find_options.to_query())

        find_options = FindOptions(sources="abcdef")
        self.assertEqual('source.name:"abcdef"', find_options.to_query())

        find_options = FindOptions(sources=None)
        self.assertEqual("", find_options.to_query())
