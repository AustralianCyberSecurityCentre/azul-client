import json
import os
import re
import sys
import tempfile
from unittest import mock

import pytest
from azul_bedrock import models_restapi
from click.testing import CliRunner
from pytest_httpx import HTTPXMock

from azul_client import Api, client
from azul_client import config as mconfig

from . import dummy_entity, dummy_entity_meta

test_file = os.path.join(os.path.dirname(__file__), "data", "testfile.txt")
test_folder = os.path.join(os.path.dirname(__file__), "data")


@pytest.fixture
def api() -> Api:
    """Fixture container the initialised api."""
    mconfig.location = ""
    cur_api = Api(
        mconfig.Config(
            auth_type="none",
            oidc_url="http://localhost",
            auth_client_id="servicer",
        )
    )
    client.api = cur_api
    return cur_api


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def mod_client(api: Api):
    client.api = api


def test_get(api: Api, runner: CliRunner, httpx_mock: HTTPXMock):
    """Just testing that it runs, not its output."""
    httpx_mock.add_response(method="POST", json={"data": {"items": []}})
    runner.invoke(
        client.get,
        [],
        catch_exceptions=False,
    )


def test_get_meta(api: Api, runner: CliRunner, httpx_mock: HTTPXMock, dummy_entity_meta: dict):
    """Tests that the get-meta command correctly returns formatted JSON for a given binary."""
    httpx_mock.add_response(
        method="GET",
        url=f"{api.config.azul_url}/api/v0/binaries/randomsha256?bucket_size=100",
        json={"data": dummy_entity_meta},
    )

    res = runner.invoke(
        client.get_meta,
        "randomsha256",
    )
    print(res.stdout)
    print(res.exception)
    assert res.exit_code == 0

    # Assert that the response is valid JSON (as this shouldn't be an interactive terminal)
    assert "security" in json.loads(res.stdout)


def test_get_meta_pretty(api: Api, runner: CliRunner, httpx_mock: HTTPXMock, dummy_entity_meta: dict):
    """Tests that pretty printing a binary works correctly."""
    httpx_mock.add_response(
        method="GET",
        url=f"{api.config.azul_url}/api/v0/binaries/randomsha256?bucket_size=100",
        json={"data": dummy_entity_meta},
    )

    # Check that forcing pretty printing returns a Pydantic repr - this won't be paginated, but this isn't
    # reasonable to test here (as it is OS dependent)
    res = runner.invoke(client.get_meta, ["randomsha256", "--pretty"])
    print(res.stdout)
    print(res.exception)
    assert res.exit_code == 0

    assert res.stdout.startswith("BinaryMetadata(")


def test_get_meta_file(api: Api, runner: CliRunner, httpx_mock: HTTPXMock, dummy_entity_meta: dict):
    """Tests that file metadata can be written to a file."""
    httpx_mock.add_response(
        method="GET",
        url=f"{api.config.azul_url}/api/v0/binaries/randomsha256?bucket_size=100",
        json={"data": dummy_entity_meta},
    )

    # Check that content can be written to a file
    with tempfile.NamedTemporaryFile() as file:
        res = runner.invoke(client.get_meta, ["randomsha256", "--output", file.name])
        print(res)
        print(res.stdout)
        print(res.stderr)
        print(res.exception)
        assert res.exit_code == 0
        assert "saving output to path %s\n" % file.name == res.stderr

        assert "security" in json.loads(file.read())


@pytest.mark.httpx_mock(can_send_already_matched_responses=True)
def test_put(api: Api, runner: CliRunner, httpx_mock: HTTPXMock, dummy_entity: dict):
    with mock.patch("click.confirm") as c:
        c.return_value = "y"

        # we are just testing that it runs, not its output
        httpx_mock.add_response(
            url=re.compile(rf".*\/api\/v0\/binaries\/source\?.*"),
            method="POST",
            json=[dummy_entity],
        )
        # test folder
        res = runner.invoke(
            client.put,
            [test_folder, "testing", "--security", "OFFICIAL"],
        )
        print(res.stdout)
        print(res.exception)
        assert res.exit_code == 0
        assert (
            re.compile("2 files found including:\n/testfile.txt\n/deep/testfile2.txt").search(res.stdout) is not None
        )
        assert "\nSource: testing" in res.stdout
        assert "\nReferences: {}" in res.stdout
        assert "\nSecurity: OFFICIAL" in res.stdout

        # test basic
        res = runner.invoke(
            client.put,
            [test_file, "testing", "--security", "OFFICIAL"],
        )
        print(res.stdout)
        print(res.exception)
        assert res.exit_code == 0
        assert re.compile("1 files found including:\ntestfile.txt\n").search(res.stdout) is not None
        assert "\nSource: testing" in res.stdout
        assert "\nReferences: {}" in res.stdout
        assert "\nSecurity: OFFICIAL" in res.stdout

        # test with references
        res = runner.invoke(
            client.put,
            [test_file, "testing", "--ref", "user:llama", "--ref", "group:primary", "--security", "OFFICIAL"],
        )
        print(res.stdout)
        print(res.exception)
        assert res.exit_code == 0
        assert re.compile("1 files found including:\ntestfile.txt\n").search(res.stdout) is not None
        assert "\nSource: testing" in res.stdout
        assert "\nReferences: {'user': 'llama', 'group': 'primary'}" in res.stdout
        assert "\nSecurity: OFFICIAL" in res.stdout

        # test with security
        res = runner.invoke(
            client.put,
            [test_file, "testing", "--security", "OFFICIAL"],
        )
        print(res.stdout)
        print(res.exception)
        assert res.exit_code == 0
        assert re.compile("1 files found including:\ntestfile.txt\n").search(res.stdout) is not None
        assert "\nSource: testing" in res.stdout
        assert "\nReferences: {}" in res.stdout
        assert "\nSecurity: OFFICIAL" in res.stdout

        # test with timestamp
        res = runner.invoke(
            client.put,
            [test_file, "testing", "--timestamp", "20220101T000000", "--security", "OFFICIAL"],
        )
        print(res.stdout)
        print(res.exception)
        assert res.exit_code == 0
        assert re.compile("1 files found including:\ntestfile.txt\n").search(res.stdout) is not None
        assert "\nSource: testing" in res.stdout
        assert "\nReferences: {}" in res.stdout
        assert "\nTimestamp: 2022-01-01T00:00:00Z" in res.stdout
        assert "\nSecurity: OFFICIAL" in res.stdout


def _read_test_file_to_std_in():
    with open(test_file, "rb") as f:
        sys.stdin = f.read()


@pytest.mark.httpx_mock(can_send_already_matched_responses=True)
def test_put_stdin(api: Api, runner: CliRunner, httpx_mock: HTTPXMock, dummy_entity: dict):
    # we are just testing that it runs, not its output
    httpx_mock.add_response(
        url=re.compile(rf".*\/api\/v0\/binaries\/source\?.*"),
        method="POST",
        json=[dummy_entity],
    )
    _read_test_file_to_std_in()
    # test basic
    res = runner.invoke(
        client.put_stdin,
        ["Spooled-test-file1.txt", "testing", "--security", "OFFICIAL", "-y"],
    )
    print(res.stdout)
    print(res.exception)
    assert res.exit_code == 0
    assert "Filename: Spooled-test-file1.txt" in res.stdout
    assert "\nSource: testing" in res.stdout
    assert "\nReferences: {}" in res.stdout
    assert "\nSecurity: OFFICIAL" in res.stdout

    _read_test_file_to_std_in()
    # test with references
    res = runner.invoke(
        client.put_stdin,
        [
            "Spooled-test-file1.txt",
            "testing",
            "--ref",
            "user:llama",
            "--ref",
            "group:primary",
            "--security",
            "OFFICIAL",
            "-y",
        ],
    )
    print(res.stdout)
    print(res.exception)
    assert res.exit_code == 0
    assert "Filename: Spooled-test-file1.txt" in res.stdout
    assert "\nSource: testing" in res.stdout
    assert "\nReferences: {'user': 'llama', 'group': 'primary'}" in res.stdout
    assert "\nSecurity: OFFICIAL" in res.stdout

    _read_test_file_to_std_in()
    # test with security
    res = runner.invoke(
        client.put_stdin,
        [
            "Spooled-test-file1.txt",
            "testing",
            "--security",
            "OFFICIAL",
            "-y",
        ],
    )
    print(res.stdout)
    print(res.exception)
    assert res.exit_code == 0
    assert "Filename: Spooled-test-file1.txt" in res.stdout
    assert "\nSource: testing" in res.stdout
    assert "\nReferences: {}" in res.stdout
    assert "\nSecurity: OFFICIAL" in res.stdout

    _read_test_file_to_std_in()
    # test with timestamp
    res = runner.invoke(
        client.put_stdin,
        [
            "Spooled-test-file1.txt",
            "testing",
            "--timestamp",
            "20220101T000000",
            "--security",
            "OFFICIAL",
            "-y",
        ],
    )
    print(res.stdout)
    print(res.exception)
    assert res.exit_code == 0
    assert "Filename: Spooled-test-file1.txt" in res.stdout
    assert "\nSource: testing" in res.stdout
    assert "\nReferences: {}" in res.stdout
    assert "\nTimestamp: 2022-01-01T00:00:00Z" in res.stdout
    assert "\nSecurity: OFFICIAL" in res.stdout


@pytest.mark.httpx_mock(can_send_already_matched_responses=True)
def test_put_child(api: Api, runner: CliRunner, httpx_mock: HTTPXMock, dummy_entity: dict):
    with mock.patch("click.confirm") as c:
        c.return_value = "y"

        # we are just testing that it runs, not its output
        httpx_mock.add_response(
            url=re.compile(rf".*\/api\/v0\/binaries\/child\?.*"),
            method="POST",
            json=[dummy_entity],
        )

        # Test base case with folder
        res = runner.invoke(
            client.put_child,
            [
                test_folder,
                "--parent",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                "--relationship",
                "action:transformed",
                "--security",
                "OFFICIAL",
            ],
        )
        assert res.exit_code == 0
        assert (
            re.compile("2 files found including:\n/testfile.txt\n/deep/testfile2.txt").search(res.stdout) is not None
        )
        assert "\nParent: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" in res.stdout
        assert "\nRelationship: {'action': 'transformed'}" in res.stdout
        assert "\nSecurity: OFFICIAL" in res.stdout

        # Test base case with a valid file
        res = runner.invoke(
            client.put_child,
            [
                test_file,
                "--parent",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                "--relationship",
                "action:transformed",
                "--security",
                "OFFICIAL",
            ],
        )
        assert res.exit_code == 0
        assert re.compile("1 files found including:\ntestfile.txt\n").search(res.stdout) is not None
        assert "\nParent: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" in res.stdout
        assert "\nRelationship: {'action': 'transformed'}" in res.stdout
        assert "\nSecurity: OFFICIAL" in res.stdout

        # Test multiple references
        res = runner.invoke(
            client.put_child,
            [
                test_file,
                "--parent",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                "--relationship",
                "action:transformed",
                "--relationship",
                "cool:action",
                "--security",
                "OFFICIAL",
            ],
        )
        assert res.exit_code == 0
        assert re.compile("1 files found including:\ntestfile.txt\n").search(res.stdout) is not None
        assert "\nParent: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" in res.stdout
        assert "\nRelationship: {'action': 'transformed', 'cool': 'action'}" in res.stdout
        assert "\nSecurity: OFFICIAL" in res.stdout

        # Test timestamp
        res = runner.invoke(
            client.put_child,
            [
                test_file,
                "--parent",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                "--relationship",
                "action:transformed",
                "--security",
                "OFFICIAL",
                "--timestamp",
                "20220101T000000",
            ],
        )
        assert res.exit_code == 0
        assert re.compile("1 files found including:\ntestfile.txt\n").search(res.stdout) is not None
        assert "\nParent: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" in res.stdout
        assert "\nRelationship: {'action': 'transformed'}" in res.stdout
        assert "\nTimestamp: 2022-01-01T00:00:00Z" in res.stdout
        assert "\nSecurity: OFFICIAL" in res.stdout

        # Test invalid sha256
        res = runner.invoke(
            client.put_child,
            [
                test_file,
                "--parent",
                "notsha256",
                "--relationship",
                "action:transformed",
                "--security",
                "OFFICIAL",
            ],
        )
        assert res.exit_code == 1


def _setup_find_mock(api: Api, httpx_mock: HTTPXMock):
    httpx_mock.reset()
    httpx_mock.add_response(
        url=re.compile(rf"{api.config.azul_url}/api/v0/binaries.*"),
        method="POST",
        status_code=200,
        json={
            "data": models_restapi.EntityFind(
                items_count=1,
                items=[
                    models_restapi.EntityFindItem(
                        key="abc123",
                        exists=True,
                        has_content=True,
                        sha256="abc123",
                    )
                ],
            ).model_dump()
        },
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


def test_get(api: Api, runner: CliRunner, httpx_mock: HTTPXMock, dummy_entity: dict):
    # Test base case simple query with all options
    _setup_find_mock(api, httpx_mock)

    res = runner.invoke(
        client.get,
        ["--term", "source.name:testing"],
    )
    assert res.exit_code == 0, res.stdout
    _make_find_assertions(httpx_mock, ["source.name:testing"], max_entities=[str(100)], sort_asc=["false"])

    # Test output directory
    _setup_find_mock(api, httpx_mock)
    "/api/v0/binaries/{sha256}/content"
    httpx_mock.add_response(
        url=f"{api.config.azul_url}/api/v0/binaries/abc123/content",
        method="GET",
        status_code=200,
        content=b"not-a-real-file",
    )

    with tempfile.TemporaryDirectory() as cur_dir:
        number_of_files_originally = len(os.listdir(cur_dir))
        res = runner.invoke(
            client.get,
            ["--output", cur_dir, "--term", "source.name:testing"],
        )
        assert res.exit_code == 0, res.stdout
        number_of_files_after = len(os.listdir(cur_dir))
        assert number_of_files_originally + 1 == number_of_files_after
    _make_find_assertions(httpx_mock, ["source.name:testing"], max_entities=[str(100)], sort_asc=["false"])

    httpx_mock.reset()
    # Test all options.
    _setup_find_mock(api, httpx_mock)

    res = runner.invoke(
        client.get,
        [
            "--term",
            "source.name:testing",
            "--max",
            "10",
            "--sort-by",
            str(models_restapi.FindBinariesSortEnum.timestamp),
            "--sort-asc",
        ],
    )
    assert res.exit_code == 0, res.stdout
    _make_find_assertions(
        httpx_mock,
        ["source.name:testing"],
        max_entities=[str(10)],
        sort_prop=[models_restapi.FindBinariesSortEnum.timestamp],
        sort_asc=["true"],
    )
