"""Regression tests for upload file ownership and cleanup."""

from io import BytesIO
from pathlib import Path
from tempfile import SpooledTemporaryFile

import pytest

from azul_client.api.binaries_data import AugmentedStream, _OpenAugmentedStreams, _OpenFile


@pytest.mark.parametrize("path_type", [str, Path])
@pytest.mark.parametrize("fail", [False, True])
def test_owned_file_closed_on_exit(tmp_path, path_type, fail):
    path = tmp_path / "sample.bin"
    path.write_bytes(b"sample")
    wrapper = _OpenFile(path_type(path))
    handle = None
    try:
        try:
            with wrapper as handle:
                assert handle.read() == b"sample"
                if fail:
                    raise RuntimeError("upload failed")
        except RuntimeError:
            if not fail:
                raise
        assert handle.closed
        wrapper.close()  # Cleanup remains safe when called again.
    finally:
        if handle is not None:
            handle.close()


@pytest.mark.parametrize("factory", [BytesIO, SpooledTemporaryFile])
@pytest.mark.parametrize("fail", [False, True])
def test_caller_owned_stream_remains_open(factory, fail):
    with factory() as stream:
        stream.write(b"sample")
        stream.seek(0)
        try:
            with _OpenFile(stream) as handle:
                assert handle is stream
                if fail:
                    raise RuntimeError("upload failed")
        except RuntimeError:
            if not fail:
                raise
        assert not stream.closed
        assert stream.read() == b"sample"


@pytest.mark.parametrize("path_type", [str, Path])
def test_augmented_stream_cleanup(tmp_path, path_type):
    path = tmp_path / "stream.bin"
    path.write_bytes(b"stream")
    stream = AugmentedStream(label="test", file_name="stream.bin", contents_file_path=path_type(path))
    handle = None
    try:
        with _OpenAugmentedStreams([stream]) as streams:
            handle = streams[0].contents_file_path.open()
            assert handle.read() == b"stream"
        assert handle.closed
    finally:
        if handle is not None:
            handle.close()
