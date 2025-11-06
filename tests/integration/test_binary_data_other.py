import io
import time

from azul_bedrock import models_network as azm
from azul_bedrock import models_restapi

from azul_client.api.binaries_data import AugmentedStream
from azul_client.exceptions import BadResponse404

from .base_test import BaseApiTest


class TestBinaryDataOther(BaseApiTest):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # NOTE - not recommend to change this by even a character as it will break a lot of tests
        cls.std_content = io.BytesIO(b"Content for strings\n and hex view\n that is testable!")
        cls.std_kwargs = {
            "source_id": cls.known_source,
            "filename": "test_binary_other_file.txt",
            "security": cls.default_security,
            "references": cls.upload_ref,
        }
        cls.aug_value = b"augvalue"

        # Create a binary with an augmented stream.
        resp = cls.api.binaries_data.upload(
            cls.std_content,
            **cls.std_kwargs,
            augmented_streams=[
                AugmentedStream(
                    label=azm.DataLabel.TEST,
                    file_name="aug_stream1.txt",
                    contents_file_path=io.BytesIO(cls.aug_value),
                ),
            ],
            refresh=True,
        )

        # Store sha256's in text because augmented stream hash isn't in response.
        cls.binary_other_sha256 = "9b9093d00cd0cb1b2542642c5042badccaa49b8bf258731141bba5633c0c6bd5"
        cls.binary_aug_sha256 = "436f659951e835408044d665ec174e6bf8d4ba1899c556e41a48c54b24883717"

        found = False
        max_total_attempts = 3
        while not found:
            max_total_attempts += 1
            start_time = time.time()
            max_wait = 15  # wait up to 15 seconds for binaries to be in the system.
            while start_time + max_wait > time.time():
                if cls.api.binaries_meta.check_meta(cls.binary_other_sha256):
                    if cls.api.binaries_data.check_data(cls.binary_other_sha256):
                        found = True
                        break
                time.sleep(2)

        if not found:
            raise Exception(
                f"Failed to verify binary_data_other was uploaded to azul in {max_wait} seconds.\n !"
                + f"File hashe is {cls.binary_other_sha256}"
            )

    def test_verify_sha256s_length(self):
        """Ensure the sha256's are the appropriate length.

        This covers the tests that loop over sha256 to make sure their assertions are actually getting called.
        For the actual length refer to the module setup in __init__
        """
        self.assertGreaterEqual(len(self.sha256s), 10)

    def test_bulk_download(self):
        content = self.api.binaries_data.download_bulk(self.sha256s)
        for sha in self.sha256s:
            self.assertIn(bytes(sha, "utf-8"), content)

    def test_download(self):
        response1 = self.api.binaries_data.download(self.sha256s[0])
        # Ensure first 4 bytes are the start of the cart file.
        self.assertEqual(response1[:4], b"CART")
        response2 = self.api.binaries_data.download(self.sha256s[1])
        self.assertEqual(response2[:4], b"CART")

    def test_check_data(self):
        for i, sha in enumerate(self.sha256s):
            exists = self.api.binaries_data.check_data(sha)
            self.assertEqual(exists, True, f"The sha256 {sha} at index {i} does not exist!")

        exists = self.api.binaries_data.check_data(self.non_existent_sha256)
        self.assertEqual(exists, False)

    def test_expedite_processing(self):
        """Expedite processing."""
        first_sha256 = self.sha256s[0]
        self.api.binaries_data.expedite_processing(first_sha256)
        self.api.binaries_data.expedite_processing(first_sha256, bypass_cache=True)
        self.api.binaries_data.expedite_processing(self.non_existent_sha256)

    def test_download_augmented_stream(self):
        aug_data = self.api.binaries_data.download_augmented_stream(self.binary_other_sha256, self.binary_aug_sha256)
        self.assertEqual(aug_data, self.aug_value)

        # Non-existent binary and augmented stream
        with self.assertRaises(BadResponse404):
            aug_data = self.api.binaries_data.download_augmented_stream(
                self.non_existent_sha256, self.non_existent_sha256
            )

    def test_download_hex(self):
        # Test getting the whole hex file.
        resp = self.api.binaries_data.download_hex(self.binary_other_sha256)
        self.assertEqual(len(resp.hex_strings), 4)
        self.assertEqual(resp.hex_strings[0].address, 0)
        hex_val = ["43", "6F", "6E", "74", "65", "6E", "74", "20", "66", "6F", "72", "20", "73", "74", "72", "69"]
        self.assertEqual(resp.hex_strings[0].hex, hex_val)

        self.assertEqual(resp.header.address, "ADDRESS")

        header_val = ["00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "0A", "0B", "0C", "0D", "0E", "0F"]
        self.assertEqual(resp.header.hex, header_val)

        self.assertEqual(resp.next_offset, 52)
        self.assertEqual(resp.content_length, 52)
        self.assertFalse(resp.has_more)

        # Test shortform
        resp = self.api.binaries_data.download_hex(self.binary_other_sha256, shortform=True)
        self.assertEqual(len(resp.hex_strings), 4)
        self.assertEqual(resp.hex_strings[0].address, 0)
        self.assertEqual(resp.hex_strings[0].hex, "436f 6e74 656e 7420 666f 7220 7374 7269")

        self.assertEqual(resp.header.address, "ADDRESS")
        self.assertEqual(resp.header.hex, "0001 0203 0405 0607 0809 0a0b 0c0d 0e0f")

        self.assertEqual(resp.next_offset, 52)
        self.assertEqual(resp.content_length, 52)
        self.assertFalse(resp.has_more)

        # Test all other parameters
        resp = self.api.binaries_data.download_hex(self.binary_other_sha256, offset=1, max_bytes_to_read=10)
        print(resp)
        self.assertEqual(len(resp.hex_strings), 1)
        self.assertEqual(resp.hex_strings[0].hex, hex_val[1:11] + ["", "", "", "", "", ""])

        self.assertTrue(resp.next_offset, 11)
        self.assertTrue(resp.content_length, 52)
        self.assertTrue(resp.has_more)

    def assert_binary_strings_equal(
        self, response: models_restapi.BinaryStrings, expected: models_restapi.BinaryStrings, test_ref: str
    ):
        print("---")
        print(test_ref + ":")
        print(response)
        self.assertEqual(response.has_more, expected.has_more, "has_more doesn't match")
        self.assertEqual(response.next_offset, expected.next_offset, "Offsets don't match")
        self.assertEqual(response.strings, expected.strings, "strings don't match")
        print("---")

    def test_get_strings(self):
        og_expected_result = models_restapi.BinaryStrings(
            strings=[
                models_restapi.SearchResult(
                    string="Content for strings", offset=0, length=19, encoding=models_restapi.SearchResultType.ASCII
                ),
                models_restapi.SearchResult(
                    string=" and hex view", offset=20, length=13, encoding=models_restapi.SearchResultType.ASCII
                ),
                models_restapi.SearchResult(
                    string=" that is testable!", offset=34, length=18, encoding=models_restapi.SearchResultType.ASCII
                ),
            ],
            has_more=False,
            next_offset=52,
        )
        resp = self.api.binaries_data.get_strings(self.binary_other_sha256)
        self.assert_binary_strings_equal(resp, og_expected_result, "base case")

        # All the different possible parameters
        # Min_length
        expected_result = og_expected_result.model_copy(deep=True)
        expected_result.strings = [expected_result.strings[0], expected_result.strings[2]]
        self.assert_binary_strings_equal(
            self.api.binaries_data.get_strings(self.binary_other_sha256, min_length=15), expected_result, "min length"
        )

        # Max Length
        expected_result = og_expected_result.model_copy(deep=True)
        expected_result.strings = [expected_result.strings[1]]
        self.assert_binary_strings_equal(
            self.api.binaries_data.get_strings(self.binary_other_sha256, max_length=15), expected_result, "max length"
        )

        # Offset
        expected_result = og_expected_result.model_copy(deep=True)
        expected_result.strings = expected_result.strings[1:]
        self.assert_binary_strings_equal(
            self.api.binaries_data.get_strings(self.binary_other_sha256, offset=19), expected_result, "Offset"
        )

        # Max bytes to read
        expected_result = models_restapi.BinaryStrings(
            strings=[
                models_restapi.SearchResult(
                    string="Conte", offset=0, length=5, encoding=models_restapi.SearchResultType.ASCII
                )
            ],
            has_more=False,
            next_offset=5,
        )
        self.assert_binary_strings_equal(
            self.api.binaries_data.get_strings(self.binary_other_sha256, max_bytes_to_read=5),
            expected_result,
            "Max bytes to read",
        )

        # take_n_strings
        expected_result = models_restapi.BinaryStrings(
            strings=[
                models_restapi.SearchResult(
                    string="Content for strings", offset=0, length=19, encoding=models_restapi.SearchResultType.ASCII
                )
            ],
            has_more=True,
            next_offset=20,
        )
        self.assert_binary_strings_equal(
            self.api.binaries_data.get_strings(self.binary_other_sha256, take_n_strings=1),
            expected_result,
            "take_n_strings",
        )

        # filter first string
        expected_result = og_expected_result.model_copy(deep=True)
        expected_result.strings = [expected_result.strings[0]]
        self.assert_binary_strings_equal(
            self.api.binaries_data.get_strings(self.binary_other_sha256, filter="Content for strings"),
            expected_result,
            "filter first string",
        )

        # filter regex
        expected_result = og_expected_result.model_copy(deep=True)
        expected_result.strings = [expected_result.strings[2]]
        self.assert_binary_strings_equal(
            self.api.binaries_data.get_strings(self.binary_other_sha256, regex=r"[^a-zA-Z\d\s:]"),
            expected_result,
            "filter regex",
        )

    def test_search_hex(self):
        og_expected_result = models_restapi.BinaryStrings(
            strings=[
                models_restapi.SearchResult(
                    string="te", offset=3, length=2, encoding=models_restapi.SearchResultType.Hex
                ),
                models_restapi.SearchResult(
                    string="te", offset=43, length=2, encoding=models_restapi.SearchResultType.Hex
                ),
            ],
            has_more=False,
            next_offset=52,
        )
        search_string = "7465"
        # Searching for "te" in hex
        resp = self.api.binaries_data.search_hex(self.binary_other_sha256, search_string)
        self.assert_binary_strings_equal(resp, og_expected_result, "base case")

        # offset
        expected_result = og_expected_result.model_copy(deep=True)
        expected_result.strings = [expected_result.strings[1]]
        self.assert_binary_strings_equal(
            self.api.binaries_data.search_hex(self.binary_other_sha256, search_string, offset=8),
            expected_result,
            "offset",
        )

        # max_bytes_to_read
        expected_result = og_expected_result.model_copy(deep=True)
        expected_result.strings = [expected_result.strings[0]]
        expected_result.next_offset = 6
        self.assert_binary_strings_equal(
            self.api.binaries_data.search_hex(self.binary_other_sha256, search_string, max_bytes_to_read=6),
            expected_result,
            "max_bytes_to_read",
        )

        # take_n_hits
        expected_result = og_expected_result.model_copy(deep=True)
        expected_result.strings = [expected_result.strings[0]]
        self.assert_binary_strings_equal(
            self.api.binaries_data.search_hex(self.binary_other_sha256, search_string, take_n_hits=1),
            expected_result,
            "take_n_hits",
        )
