import time
import urllib
from unittest import mock

import pytest
from pytest_httpx import HTTPXMock

from azul_client import Api, Config, client
from azul_client import config as mconfig
from azul_client.oidc import OIDC

jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
OIDC_URL = "http://localhost:8123"


@pytest.fixture
def api_service_api() -> Api:
    """Fixture container the initialised api."""
    mconfig.location = ""
    cur_api = Api(
        mconfig.Config(
            auth_type="service",
            oidc_url=OIDC_URL,
            auth_client_id="servicer",
            auth_client_secret="secrot",
        )
    )
    client.api = cur_api
    return cur_api


def test_via_service_token(api_service_api: Api, httpx_mock: HTTPXMock):
    endpt = api_service_api.config.oidc_url
    print(f"{endpt=}")

    wellknown = {
        "authorization_endpoint": f"{endpt}/authz",
        "token_endpoint": f"{endpt}/token",
    }
    httpx_mock.add_response(
        method="GET",
        url=endpt,
        json=wellknown,
        headers={"content-type": "application/json; blarg"},
    )
    httpx_mock.add_response(
        method="POST",
        url=endpt + "/token",
        json={"access_token": "me give access"},
        headers={"content-type": "application/json; blarg"},
    )
    token = api_service_api.auth._via_service_token()

    last_request = httpx_mock.get_requests()[-1]
    data = last_request.read()
    parsed_content = urllib.parse.parse_qs(data.decode())
    assert "me give access" == token["access_token"]
    assert "client_credentials" == parsed_content["grant_type"][0]
    assert "secrot" == parsed_content["client_secret"][0]
    assert "servicer" == parsed_content["client_id"][0]


@pytest.fixture
def api_code_ex_api() -> Api:
    """Fixture container the initialised api."""
    mconfig.location = ""
    cur_api = Api(
        mconfig.Config(
            auth_type="callback",
            oidc_url=OIDC_URL,
            auth_client_id="me",
        )
    )
    client.api = cur_api
    return cur_api


def test_via_code_callback(api_code_ex_api: Api, httpx_mock: HTTPXMock):
    endpt = api_code_ex_api.config.oidc_url
    wellknown = {
        "authorization_endpoint": f"{endpt}/authz",
        "token_endpoint": f"{endpt}/token",
    }
    httpx_mock.add_response(
        method="GET",
        url=endpt,
        json=wellknown,
        headers={"content-type": "application/json; blarg"},
    )
    httpx_mock.add_response(
        method="POST",
        url=endpt + "/token",
        json={"access_token": jwt},
        headers={"content-type": "application/json; blarg"},
    )
    with mock.patch("azul_client.oidc.callback.receive_code", lambda *k, **v: "the_code"):
        token = api_code_ex_api.auth._via_code_callback()

    last_request = httpx_mock.get_requests()[-1]
    data = last_request.read()
    parsed_content = urllib.parse.parse_qs(data.decode())
    assert jwt == token["access_token"]
    assert "authorization_code" == parsed_content["grant_type"][0]
    assert "me" == parsed_content["client_id"][0]


@pytest.fixture
def oidc_svc_callback() -> OIDC:
    mconfig.location = ""
    return OIDC(
        Config(
            auth_type="callback",
            oidc_url=OIDC_URL,
            auth_client_id="servicer",
        )
    )


@mock.patch("azul_client.oidc.oidc.OIDC._via_code_callback")
@mock.patch("azul_client.oidc.oidc.OIDC._via_refresh")
@mock.patch("azul_client.config.Config.save", lambda x: None)
def test_get_token(vr: mock.MagicMock, vcc: mock.MagicMock, oidc_svc_callback: OIDC):
    vcc.return_value = {"access_token": "token"}
    vr.return_value = {"access_token": "refreshed_token"}
    # trigger initial auth
    assert "token" == oidc_svc_callback.get_access_token()
    assert 1 == vcc.call_count
    assert 0 == vr.call_count
    # reuse existing token
    assert "token" == oidc_svc_callback.get_access_token()
    assert 1 == vcc.call_count
    assert 0 == vr.call_count
    # check that refresh token is used
    oidc_svc_callback.cfg.auth_token_time = time.time() - 60
    assert "refreshed_token" == oidc_svc_callback.get_access_token()
    assert 1 == vcc.call_count
    assert 1 == vr.call_count
    # pretend that refresh token expired, so full auth required
    vr.return_value = None
    oidc_svc_callback.cfg.auth_token_time = time.time() - 600
    assert "token" == oidc_svc_callback.get_access_token()
    assert 2 == vcc.call_count
    assert 2 == vr.call_count


@mock.patch("azul_client.oidc.oidc.OIDC._via_code_callback")
@mock.patch("azul_client.config.Config.save", lambda x: None)
def test_get_token_errors(vcc: mock.MagicMock, oidc_svc_callback: OIDC, httpx_mock: HTTPXMock):
    endpt = oidc_svc_callback.cfg.oidc_url
    vcc.return_value = {"access_token": "token", "refresh_token": "invalid"}
    # trigger initial auth
    assert "token" == oidc_svc_callback.get_access_token()
    assert 1 == vcc.call_count
    # reuse existing token
    assert "token" == oidc_svc_callback.get_access_token()
    assert 1 == vcc.call_count
    # check that refresh token failure 400 results in full reauth
    vcc.return_value = {"access_token": "redo_token", "refresh_token": "invalid"}
    wellknown = {
        "authorization_endpoint": f"{endpt}/authz",
        "token_endpoint": f"{endpt}/token",
    }
    httpx_mock.add_response(
        method="GET",
        url=endpt,
        json=wellknown,
        headers={"content-type": "application/json; blarg"},
    )
    httpx_mock.add_response(
        method="POST",
        url=endpt + "/token",
        content="refresh token expired",
        headers={"content-type": "application/json; blarg"},
        status_code=400,
    )
    oidc_svc_callback.cfg.auth_token_time = time.time() - 60
    assert "redo_token" == oidc_svc_callback.get_access_token()

    vcc.return_value = {"access_token": "redo_token2", "refresh_token": "invalid"}
    httpx_mock.add_response(
        method="POST",
        url=endpt + "/token",
        content="refresh token expired",
        headers={"content-type": "application/json; blarg"},
        status_code=401,
    )
    oidc_svc_callback.cfg.auth_token_time = time.time() - 60
    assert "redo_token2" == oidc_svc_callback.get_access_token()
