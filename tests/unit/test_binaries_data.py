import io
import json
import re

import cart
import malpz
import pytest
import python_multipart
from azul_bedrock import models_network as azm
from azul_bedrock import models_restapi
from cart.cart import CART_MAGIC, TRAC_MAGIC
from pytest_httpx import HTTPXMock

from azul_client import Api
from azul_client.api import binaries_data
from azul_client.config import Config


@pytest.fixture
def api() -> Api:
    """Fixture container the initialised api."""
    return Api(
        Config(
            auth_type="none",
            azul_url="http://localhost",
        )
    )


@pytest.fixture
def dummy_entity() -> dict:
    return azm.BinaryEvent.Entity(sha256="random").model_dump()


@pytest.fixture
def dummy_entity_meta() -> dict:
    """Taken from output of the integration test."""
    return models_restapi.BinaryMetadata(
        documents=models_restapi.BinaryDocuments(count=250, newest="2024-05-10T00:28:57.749Z"),
        security=["OFFICIAL"],
        sources=[
            models_restapi.BinarySource(
                source="testing",
                direct=[
                    models_restapi.EventSource(
                        security="OFFICIAL",
                        name="testing",
                        timestamp="2024-05-09T04:26:03.174062Z",
                        references={"user": "azul-client-test"},
                        track_source_references="testing.985fad029e65f5f68005505c8f6b57ba",
                    )
                ],
                indirect=[],
            )
        ],
        tags=[],
        parents=[],
        instances=[
            models_restapi.EntityInstance(
                key="plugin.ExifTool.binary_enriched.",
                author=models_restapi.EntityInstanceAuthor(
                    security="OFFICIAL", category="plugin", name="ExifTool", version="2024.04.29"
                ),
                action=azm.BinaryAction.Enriched,
                stream=None,
                num_feature_values=7,
            )
        ],
        features=[],
        streams=[],
        info=[
            {
                "info": {
                    "entropy": {"blocks": [], "overall": 3.4548223999466066, "block_count": 0, "block_size": 256}
                },
                "instance": "plugin.entropy.binary_enriched.",
            }
        ],
    ).model_dump()


def test_upload(api: Api, httpx_mock: HTTPXMock, dummy_entity: dict):

    httpx_mock.add_response(
        url=re.compile(rf".*\/api\/v0\/binaries\/source\?.*"),
        method="POST",
        status_code=200,
        json=[dummy_entity],
    )
    api.binaries_data.upload(
        io.BytesIO(b"content"),
        source_id="source1",
        security="",
        augmented_streams=[
            binaries_data.AugmentedStream(
                label="data1", file_name="file1.exe", contents_file_path=io.BytesIO(b"type1")
            ),
            binaries_data.AugmentedStream(
                label="data2", file_name="file2.exe", contents_file_path=io.BytesIO(b"type2")
            ),
            binaries_data.AugmentedStream(
                label="data3", file_name="file3.exe", contents_file_path=io.BytesIO(b"type3")
            ),
        ],
    )
    last_request = httpx_mock.get_request()
    body = last_request.read()
    assert b'Content-Disposition: form-data; name="stream_data"; filename="file1.exe"\r\n' in body
    assert b'Content-Disposition: form-data; name="stream_data"; filename="file2.exe"\r\n' in body
    assert b'Content-Disposition: form-data; name="stream_data"; filename="file3.exe"\r\n' in body
    assert b'Content-Disposition: form-data; name="stream_labels"\r\n\r\ndata1\r\n' in body
    assert b'Content-Disposition: form-data; name="stream_labels"\r\n\r\ndata2\r\n' in body
    assert b'Content-Disposition: form-data; name="stream_labels"\r\n\r\ndata3\r\n' in body
    assert b'Content-Disposition: form-data; name="security"\r\n\r\n\r\n' in body

    # test security
    httpx_mock.reset()
    httpx_mock.add_response(
        url=re.compile(rf".*\/api\/v0\/binaries\/source\?.*"), method="POST", status_code=200, json=[dummy_entity]
    )
    api.binaries_data.upload(
        io.BytesIO(b"content"),
        "source1",
        security="OFFICIAL",
        filename="dummy",
    )
    last_request = httpx_mock.get_request()
    body = last_request.read()
    print(body.decode("utf-8", errors="replace"))
    assert b'Content-Disposition: form-data; name="security"\r\n\r\nOFFICIAL\r\n' in body

    # more security tests
    httpx_mock.reset()
    httpx_mock.add_response(
        url=re.compile(rf".*\/api\/v0\/binaries\/source\?.*"), method="POST", status_code=200, json=[dummy_entity]
    )
    api.binaries_data.upload(
        io.BytesIO(b"content"),
        "source1",
        security="OFFICIAL//TLP:GREEN",
        filename="dummy",
    )
    body = httpx_mock.get_request().read()
    print(body.decode("utf-8", errors="replace"))
    assert b'Content-Disposition: form-data; name="security"\r\n\r\nOFFICIAL//TLP:GREEN\r\n' in body

    # test no filename
    pytest.raises(ValueError, api.binaries_data.upload, io.BytesIO(b"content"), "source1", security="OFFICIAL")

    # test bad source id
    pytest.raises(
        ValueError, api.binaries_data.upload, io.BytesIO(b"content"), "", security="OFFICIAL", filename="abc.txt"
    )


def test_upload_neutering(api: Api, httpx_mock: HTTPXMock, dummy_entity: dict):
    httpx_mock.add_response(
        url=re.compile(rf".*\/api\/v0\/binaries\/source\?.*"),
        method="POST",
        status_code=200,
        json=[dummy_entity],
    )

    # Unneutered file should be CaRT'd
    api.binaries_data.upload(
        io.BytesIO(b"content"),
        filename="testfile",
        source_id="source1",
        security="",
        augmented_streams=[],
    )
    last_request = httpx_mock.get_request()
    body = last_request.read()
    assert CART_MAGIC in body
    assert TRAC_MAGIC in body

    # CaRT'd files should be not be CaRT'd again
    httpx_mock.reset()
    httpx_mock.add_response(
        url=re.compile(rf".*\/api\/v0\/binaries\/source\?.*"),
        method="POST",
        status_code=200,
        json=[dummy_entity],
    )

    buffer = io.BytesIO()
    cart.pack_stream(io.BytesIO(b"content"), buffer, optional_header="EXTRA CART CONTENT")
    buffer.seek(0)
    api.binaries_data.upload(
        buffer,
        filename="testfile",
        source_id="source1",
        security="",
        augmented_streams=[],
    )
    last_request = httpx_mock.get_request()
    body = last_request.read()

    assert CART_MAGIC in body
    assert TRAC_MAGIC in body

    # Validate that the file itself is only CaRT'd once
    file_contents: python_multipart.multipart.File | None = None

    def field_handler(_field):
        pass

    def file_handler(file):
        nonlocal file_contents
        file_contents = file

    python_multipart.parse_form(last_request.headers, io.BytesIO(body), field_handler, file_handler)

    handle = file_contents.file_object  # type: ignore
    handle.seek(0)

    output_contents = io.BytesIO()
    (header, _) = cart.unpack_stream(handle, output_contents)

    assert b"content" == output_contents.getvalue()
    # Validate that another cart was not used to transport this content
    assert header == "EXTRA CART CONTENT"

    # Malpz'd files should remain as malpz
    httpx_mock.reset()
    httpx_mock.add_response(
        url=re.compile(rf".*\/api\/v0\/binaries\/source\?.*"),
        method="POST",
        status_code=200,
        json=[dummy_entity],
    )

    malpz_file = malpz.wrap(b"content", "security")
    api.binaries_data.upload(
        io.BytesIO(malpz_file),
        filename="testfile",
        source_id="source1",
        security="",
        augmented_streams=[],
    )
    last_request = httpx_mock.get_request()
    body = last_request.read()
    print(body)
    assert malpz.MALPZ_HEADER in body


def test_upload_dataless(api: Api, httpx_mock: HTTPXMock, dummy_entity: dict):
    httpx_mock.add_response(
        url=re.compile(rf".*\/api\/v0\/binaries\/source\/dataless\?.*"),
        method="POST",
        status_code=200,
        json=[dummy_entity],
    )
    api.binaries_data.upload_dataless(
        "6e9472fa56a71791f051613a0198a8c15948136e916641974ddcd6ca556ce7d0",
        source_id="source1",
        security="",
        augmented_streams=[
            binaries_data.AugmentedStream(
                label="data1", file_name="file1.exe", contents_file_path=io.BytesIO(b"type1")
            ),
            binaries_data.AugmentedStream(
                label="data2", file_name="file2.exe", contents_file_path=io.BytesIO(b"type2")
            ),
            binaries_data.AugmentedStream(
                label="data3", file_name="file3.exe", contents_file_path=io.BytesIO(b"type3")
            ),
        ],
    )
    last_request = httpx_mock.get_request()
    body = last_request.read()
    assert b'Content-Disposition: form-data; name="stream_data"; filename="file1.exe"\r\n' in body
    assert b'Content-Disposition: form-data; name="stream_data"; filename="file2.exe"\r\n' in body
    assert b'Content-Disposition: form-data; name="stream_data"; filename="file3.exe"\r\n' in body
    assert b'Content-Disposition: form-data; name="stream_labels"\r\n\r\ndata1\r\n' in body
    assert b'Content-Disposition: form-data; name="stream_labels"\r\n\r\ndata2\r\n' in body
    assert b'Content-Disposition: form-data; name="stream_labels"\r\n\r\ndata3\r\n' in body
    assert b'Content-Disposition: form-data; name="security"\r\n\r\n\r\n' in body

    # test security
    httpx_mock.reset()
    pytest.raises(
        ValueError,
        api.binaries_data.upload_dataless,
        "6e9472fa56a71791f051613a0198a8c15948136e916641974ddcd6ca556ce7d0",
        source_id="source1",
        security=["OFFICIAL"],
        filename="dummy",
    )

    # test bad parentID (not a sha256)
    pytest.raises(
        ValueError,
        api.binaries_data.upload_dataless,
        "fa56a71791f051613a0198a8c15948136e916641974ddcd6ca556ce7d0",
        source_id="source1",
        security=["OFFICIAL"],
        filename="dummy",
    )

    pytest.raises(
        ValueError,
        api.binaries_data.upload_dataless,
        "6e9472fa56a71791f051613a0198a8c15948136e916641974ddcd6ca556ce7dZ",
        source_id="source1",
        security=["OFFICIAL"],
        filename="dummy",
    )


def test_upload_child(api: Api, httpx_mock: HTTPXMock, dummy_entity: dict):
    httpx_mock.add_response(
        url=re.compile(rf".*\/api\/v0\/binaries\/child\?.*"), method="POST", status_code=200, json=[dummy_entity]
    )
    api.binaries_data.upload_child(
        io.BytesIO(b"content"),
        "6e9472fa56a71791f051613a0198a8c15948136e916641974ddcd6ca556ce7d0",
        {"extracted": "Extracted by 7zip."},
        filename="child_test.txt",
        security="",
    )
    last_request = httpx_mock.get_request()
    body = last_request.read()
    assert b'filename="child_test.txt"' in body
    assert b'Content-Disposition: form-data; name="filename"\r\n' in body

    # test security
    httpx_mock.reset()
    httpx_mock.add_response(
        url=re.compile(rf".*\/api\/v0\/binaries\/child\?.*"), method="POST", status_code=200, json=[dummy_entity]
    )
    api.binaries_data.upload_child(
        io.BytesIO(b"content"),
        "6e9472fa56a71791f051613a0198a8c15948136e916641974ddcd6ca556ce7d0",
        {"extracted": "Extracted by 7zip."},
        security="OFFICIAL",
        filename="child_test.txt",
    )
    last_request = httpx_mock.get_request()
    body = last_request.read()
    print(body.decode("utf-8", errors="replace"))
    assert b'Content-Disposition: form-data; name="security"\r\n\r\nOFFICIAL\r\n' in body

    # more security tests
    httpx_mock.reset()
    httpx_mock.add_response(
        url=re.compile(rf".*\/api\/v0\/binaries\/child\?.*"), method="POST", status_code=200, json=[dummy_entity]
    )
    api.binaries_data.upload_child(
        io.BytesIO(b"content"),
        "6e9472fa56a71791f051613a0198a8c15948136e916641974ddcd6ca556ce7d0",
        {"extracted": "Extracted by 7zip."},
        security="OFFICIAL//TLP:GREEN",
        filename="child_test.txt",
    )
    body = httpx_mock.get_request().read()
    print(body.decode("utf-8", errors="replace"))
    assert b'Content-Disposition: form-data; name="security"\r\n\r\nOFFICIAL//TLP:GREEN\r\n' in body

    # test no filename
    pytest.raises(
        ValueError,
        api.binaries_data.upload_child,
        io.BytesIO(b"content"),
        "6e9472fa56a71791f051613a0198a8c15948136e916641974ddcd6ca556ce7d0",
        {"extracted": "Extracted by 7zip."},
        security="OFFICIAL",
    )

    # test bad parentID
    pytest.raises(
        ValueError,
        api.binaries_data.upload_child,
        io.BytesIO(b"content"),
        "ze9472fa56a71791f051613a0198a8c15948136e916641974ddcd6ca556ce7d0",
        {"extracted": "Extracted by 7zip."},
        security="OFFICIAL",
        filename="child_test.txt",
    )

    pytest.raises(
        ValueError,
        api.binaries_data.upload_child,
        io.BytesIO(b"content"),
        "16641974ddcd6ca556ce7d0",
        {"extracted": "Extracted by 7zip."},
        security="OFFICIAL",
        filename="child_test.txt",
    )

    # Test bad reference
    pytest.raises(
        ValueError,
        api.binaries_data.upload_child,
        io.BytesIO(b"content"),
        "6e9472fa56a71791f051613a0198a8c15948136e916641974ddcd6ca556ce7d0",
        {},
        security="OFFICIAL",
        filename="child_test.txt",
    )


def test_download_bulk(api: Api, httpx_mock: HTTPXMock, dummy_entity: dict):
    httpx_mock.add_response(
        url=f"{api.config.azul_url}/api/v0/binaries/content/bulk", method="POST", status_code=200, json=[dummy_entity]
    )

    api.binaries_data.download_bulk(
        [
            "52b526411070a0a92075ea7c2575f759f480f2f4788d56300091696fc7eabb71a74a5fbca04b1934e215ca00bb6b977f6069a34588caa81f622616caacbc83bf",
        ]
    )
    body = httpx_mock.get_request().read()
    print(body.decode("utf-8", errors="replace"))
    data = json.loads(body.decode("utf-8", errors="replace"))
    assert (
        "52b526411070a0a92075ea7c2575f759f480f2f4788d56300091696fc7eabb71a74a5fbca04b1934e215ca00bb6b977f6069a34588caa81f622616caacbc83bf"
        == data["binaries"][0]
    )


def test_get_meta(api: Api, httpx_mock: HTTPXMock, dummy_entity_meta: dict):
    dummy_meta = dummy_entity_meta
    httpx_mock.add_response(
        url=re.compile(rf"{api.config.azul_url}/api/v0/binaries/[A-Za-z0-9]{{128}}"),
        method="GET",
        status_code=200,
        json={"data": dummy_meta},
    )

    api.binaries_meta.get_meta(
        "52b526411070a0a92075ea7c2575f759f480f2f4788d56300091696fc7eabb71a74a5fbca04b1934e215ca00bb6b977f6069a34588caa81f622616caacbc83bf"
    )
    path = httpx_mock.get_request().url.path
    assert (
        "52b526411070a0a92075ea7c2575f759f480f2f4788d56300091696fc7eabb71a74a5fbca04b1934e215ca00bb6b977f6069a34588caa81f622616caacbc83bf"
        in path
    )


def _setup_find_mock(api: Api, httpx_mock: HTTPXMock):
    httpx_mock.reset()
    httpx_mock.add_response(
        url=re.compile(rf"{api.config.azul_url}/api/v0/binaries.*"),
        method="POST",
        status_code=200,
        json={"data": models_restapi.EntityFind(items_count=0, items=[]).model_dump()},
    )


def _make_find_assertions(
    httpx_mock: HTTPXMock,
    term: str = [],
    max_entities: int = [],
    sort_prop: models_restapi.FindBinariesSortEnum = [],
    sort_asc: bool = [],
):
    req = httpx_mock.get_requests()
    assert req[0].url.params.get_list("term") == term
    assert req[0].url.params.get_list("max_entities") == max_entities
    assert req[0].url.params.get_list("sort") == sort_prop
    assert req[0].url.params.get_list("sort_asc") == sort_asc


def test_find(api: Api, httpx_mock: HTTPXMock):
    _setup_find_mock(api, httpx_mock)
    api.binaries_meta.find(term="source.name:testing")
    _make_find_assertions(httpx_mock, ["source.name:testing"])

    _setup_find_mock(api, httpx_mock)
    api.binaries_meta.find(
        term="source.name:testing",
        max_entities=10,
        sort_prop=models_restapi.FindBinariesSortEnum.timestamp,
        sort_asc=True,
    )
    _make_find_assertions(
        httpx_mock,
        ["source.name:testing"],
        max_entities=[str(10)],
        sort_prop=[models_restapi.FindBinariesSortEnum.timestamp],
        sort_asc=["true"],
    )

    _setup_find_mock(api, httpx_mock)
    api.binaries_meta.find(
        term="source.name:testing", sort_prop=models_restapi.FindBinariesSortEnum.source_timestamp, sort_asc=False
    )
    _make_find_assertions(
        httpx_mock,
        ["source.name:testing"],
        max_entities=[],
        sort_prop=[models_restapi.FindBinariesSortEnum.source_timestamp],
        sort_asc=["false"],
    )
