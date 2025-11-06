import json

import pytest
from pytest_httpx import HTTPXMock

from azul_client import Api, Config
from azul_client import config as mconfig


@pytest.fixture
def api() -> Api:
    mconfig.location = ""
    return Api(
        Config(
            auth_type="none",
            azul_url="http://localhost:8123",
        )
    )


def test_normalise_basic(api: Api, httpx_mock: HTTPXMock):
    httpx_mock.add_response(
        method="POST",
        url=f"{api.config.azul_url}/api/v1/security/normalise",
        status_code=200,
        content=b'"OFFICIAL TLP:GREEN"',
    )
    normalised_security = api.security.normalise("OFFICIAL//TLP:GREEN")
    body = json.loads(httpx_mock.get_request().read())
    assert body == {"security": "OFFICIAL//TLP:GREEN"}
    print(normalised_security)
    assert normalised_security == "OFFICIAL TLP:GREEN"
