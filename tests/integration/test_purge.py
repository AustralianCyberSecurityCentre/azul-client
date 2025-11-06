import time

from azul_bedrock import models_restapi

from . import module_sha256s, module_source
from .base_test import BaseApiTest


class TestPurgeApi(BaseApiTest):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.calculated_username = cls.api.users.get_user_info().username

    def test_purge_submission(self):
        """Test you could purge a submission.

        NOTE - you must be admin for this to work.
        """
        # Find needed metadata for submission
        binary_meta = self.api.binaries_meta.get_meta(module_sha256s[0])
        submission_meta = binary_meta.sources[0].direct[0]

        # Simulate purge But timestamp isn't provided
        self.assertRaises(
            ValueError, self.api.purge.purge_submission, submission_meta.track_source_references, timestamp=None
        )

        # Simulate purge But timestamp is invalid
        self.assertRaises(
            ValueError, self.api.purge.purge_submission, submission_meta.track_source_references, timestamp="None"
        )
        # Bad date that should get 0 results.
        resp_bad_date = self.api.purge.purge_submission(submission_meta.track_source_references, timestamp="2024")
        self.assertEqual(resp_bad_date.events, 0)

        # Simulate purge for timestamp
        resp_time = self.api.purge.purge_submission(
            submission_meta.track_source_references, timestamp=submission_meta.timestamp
        )
        self.assertGreaterEqual(resp_time.events, 1)

    def test_purge_link(self):
        """Simulate purging a child binary because actually purging it would cause race condition issues.

        NOTE - you must be admin for this to work.
        """
        # Add a child
        parent = module_sha256s[0]
        child_entity = self.api.binaries_data.upload_child(
            b"child_file_for_file1",
            parent,
            {"extracted": "by-test-user"},
            filename="child1.txt",
            security=self.default_security,
        )

        child_id = child_entity.sha256
        self.assertEqual(child_id, "30c275c36b2960732a63cf800408cde629c6b8b136b4f01d99610609696689ad")
        time.sleep(2)

        binary_meta = self.api.binaries_meta.get_meta(child_entity.sha256)
        self.assertGreaterEqual(len(binary_meta.parents), 1)

        # Simulate purging the child.
        resp: models_restapi.PurgeSimulation = self.api.purge.purge_link(binary_meta.parents[0].track_link)
        self.assertGreaterEqual(resp.events, 1)
