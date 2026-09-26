"""Dataless uploads must reject invalid hashes before making a request."""

from unittest.mock import Mock

import pytest

from azul_client import Api
from azul_client.config import Config


@pytest.mark.parametrize("binary_id", [None, "", "not-a-hash", "a" * 63, "a" * 65, "g" * 64, "a" * 64 + "\n"])
def test_invalid_hash_rejected_before_upload(binary_id, monkeypatch):
    api = Api(Config(auth_type="none", azul_url="http://localhost"))
    upload = Mock()
    monkeypatch.setattr(api.binaries_data, "_base_upload", upload)
    with pytest.raises(ValueError, match="valid sha256"):
        api.binaries_data.upload_dataless(binary_id, source_id="source1", security="OFFICIAL")
    upload.assert_not_called()


@pytest.mark.parametrize("binary_id", ["a" * 64, "A" * 64, "0123456789abcdef" * 4])
def test_valid_hash_forwarded_unchanged(binary_id, monkeypatch):
    api = Api(Config(auth_type="none", azul_url="http://localhost"))
    upload = Mock()
    monkeypatch.setattr(api.binaries_data, "_base_upload", upload)
    result = api.binaries_data.upload_dataless(binary_id, source_id="source1", security="OFFICIAL")
    assert result is upload.return_value
    upload.assert_called_once()
    assert upload.call_args.kwargs["body"]["sha256"] == binary_id
