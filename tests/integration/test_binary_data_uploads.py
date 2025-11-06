import copy
import io
import unittest
from tempfile import SpooledTemporaryFile

from azul_bedrock import models_network as azm

from azul_client import Api
from azul_client.api import binaries_data
from azul_client.config import get_config
from azul_client.exceptions import BadResponse, BadResponse404

from . import module_ref, module_sha256s, module_source


class BaseUploadTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.api = Api(get_config())
        cls.source = module_source
        cls.ref = module_ref
        cls.classifications = ["OFFICIAL//TLP:GREEN"]
        cls.non_existent_sha256 = "ed7002b439e9ac845f22357d822bac1444730fbdb6016d3ec9432297b9ec9f74"
        cls.submit_settings = {"test": "test_setting"}


class TestBinaryUpload(BaseUploadTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

    def setUp(self):
        self.std_content = io.BytesIO(b"content")
        self.std_kwargs = {
            "source_id": self.source,
            "filename": "test_basic_upload.txt",
            "security": "OFFICIAL",
            "references": self.ref,
        }

    def basic_response_check(self, entity: azm.BinaryEvent.Entity):
        self.assertEqual(entity.sha256, "ed7002b439e9ac845f22357d822bac1444730fbdb6016d3ec9432297b9ec9f73")
        self.assertEqual(entity.file_format_legacy, "Text")
        self.assertEqual(entity.mime, "text/plain")

    def test_basic_upload(self):
        resp = self.api.binaries_data.upload(self.std_content, **self.std_kwargs)
        self.basic_response_check(resp)

    def test_basic_upload_with_settings(self):
        resp = self.api.binaries_data.upload(self.std_content, **self.std_kwargs, submit_settings=self.submit_settings)
        self.basic_response_check(resp)

    def test_basic_upload_spooled_file(self):
        with SpooledTemporaryFile(max_size=100000) as spooledFile:
            spooledFile.write(self.std_content.read())
            spooledFile.seek(0)
            resp = self.api.binaries_data.upload(spooledFile, **self.std_kwargs)
            self.basic_response_check(resp)

    def test_upload_with_security(self):
        self.std_kwargs["security"] = self.classifications[0]
        resp = self.api.binaries_data.upload(self.std_content, **self.std_kwargs)
        self.basic_response_check(resp)

    def test_upload_with_aug(self):
        resp = self.api.binaries_data.upload(
            self.std_content,
            **self.std_kwargs,
            augmented_streams=[
                binaries_data.AugmentedStream(
                    label=azm.DataLabel.TEST, file_name="file1.exe", contents_file_path=io.BytesIO(b"type1")
                ),
                binaries_data.AugmentedStream(
                    label=azm.DataLabel.ASSEMBLYLINE, file_name="file2.exe", contents_file_path=io.BytesIO(b"type2")
                ),
                binaries_data.AugmentedStream(
                    label=azm.DataLabel.TEXT, file_name="file3.exe", contents_file_path=io.BytesIO(b"type3")
                ),
            ],
        )
        self.basic_response_check(resp)

    def test_invalid_source_upload(self):
        self.std_kwargs["source_id"] = "random_source_id_that_doesnt_exist"
        self.assertRaises(
            BadResponse,
            self.api.binaries_data.upload,
            self.std_content,
            **self.std_kwargs,
        )


class TestBinaryUploadDataless(BaseUploadTest):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.parent_sha256 = module_sha256s[0]

    def setUp(self):
        self.std_kwargs = {
            "security": "OFFICIAL",
            "references": self.ref,
        }
        self.non_existent_sha256 = "ed7002b439e9ac845f22357d822bac1444730fbdb6016d3ec9432297b9ec9f74"

    def basic_response_check(self, entity: azm.BinaryEvent.Entity):
        self.assertEqual(entity.sha256, self.parent_sha256)
        self.assertEqual(entity.file_format_legacy, "Text")
        self.assertEqual(entity.mime, "text/plain")

    def test_basic_upload(self):
        resp = self.api.binaries_data.upload_dataless(self.parent_sha256, self.source, **self.std_kwargs)
        self.basic_response_check(resp)

    def test_upload_with_security(self):
        self.std_kwargs["security"] = self.classifications[0]
        resp = self.api.binaries_data.upload_dataless(
            self.parent_sha256, self.source, filename="newfilename.txt", **self.std_kwargs
        )
        self.basic_response_check(resp)

    def test_upload_with_aug(self):
        resp = self.api.binaries_data.upload_dataless(
            self.parent_sha256,
            self.source,
            augmented_streams=[
                binaries_data.AugmentedStream(
                    label=azm.DataLabel.TEXT, file_name="file1.exe", contents_file_path=io.BytesIO(b"type1")
                ),
                binaries_data.AugmentedStream(
                    label=azm.DataLabel.TEST, file_name="file2.exe", contents_file_path=io.BytesIO(b"type2")
                ),
            ],
            **self.std_kwargs,
        )
        self.basic_response_check(resp)

    def test_invalid_source_upload(self):
        self.assertRaises(
            BadResponse,
            self.api.binaries_data.upload_dataless,
            self.parent_sha256,
            "random_source_id_that_doesnt_exist",
            **self.std_kwargs,
        )

    def test_invalid_parent_sha256(self):
        self.assertRaises(
            BadResponse404,
            self.api.binaries_data.upload_dataless,
            self.non_existent_sha256,  # in theory this hash isn't in the system
            self.source,
            **self.std_kwargs,
        )


class TestBinaryUploadChild(BaseUploadTest):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.parent_sha256s = copy.copy(module_sha256s)

    def setUp(self):
        self.std_kwargs = {"security": "OFFICIAL", "filename": "child-file.txt"}
        self.relationship = {"test-parent": "parent-in-test"}
        self.child_content = io.BytesIO(b"Child-content")
        self.parent_sha256 = self.parent_sha256s.pop()

    def basic_response_check(self, entity: azm.BinaryEvent.Entity):
        self.assertEqual(entity.sha256, "d2dba47e53f9d97060b3e5fee9416d06177aee5e2e9488f8643d1c3f55cc9f6b")
        self.assertEqual(entity.file_format_legacy, "Text")
        self.assertEqual(entity.mime, "text/plain")

    def test_basic_upload(self):
        resp = self.api.binaries_data.upload_child(
            self.child_content,
            self.parent_sha256,
            self.relationship,
            **self.std_kwargs,
            submit_settings=self.submit_settings,
        )
        self.basic_response_check(resp)

    def test_upload_with_security(self):
        self.std_kwargs["security"] = self.classifications[0]
        resp = self.api.binaries_data.upload_child(
            self.child_content, self.parent_sha256, self.relationship, **self.std_kwargs
        )
        self.basic_response_check(resp)

    def test_no_parent_child_relationship(self):
        self.assertRaises(
            ValueError,
            self.api.binaries_data.upload_child,
            self.child_content,
            self.parent_sha256,
            {},
            **self.std_kwargs,
        )

    def test_invalid_parent_sha256(self):
        self.assertRaises(
            BadResponse,
            self.api.binaries_data.upload_child,
            self.child_content,
            self.non_existent_sha256,
            self.relationship,
            **self.std_kwargs,
        )
