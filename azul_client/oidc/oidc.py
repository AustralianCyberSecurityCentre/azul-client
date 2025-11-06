"""Auth handling."""

import base64
import hashlib
import json
import os
import random
import re
import sys
import time
import urllib.parse

import httpx

from azul_client import config

from . import callback


def _get_json(resp: httpx.Response, dbg: str) -> dict:
    """Try to read json data out of the web request."""
    if resp.status_code != 200:
        raise Exception(f"Failed to fetch {dbg} - Code: {resp.status_code} - Content:\n{resp.content[:1000]}")
    if "application/json" not in resp.headers.get("content-type"):
        raise Exception(
            f"Failed to fetch {dbg} - response mime is not json ({resp.headers.get('content-type')}). "
            f"Is '{resp.url}' the correct endpoint? - "
            f"Content:\n{resp.content[:1000]}"
        )
    try:
        data = resp.json()
    except json.decoder.JSONDecodeError:
        raise Exception(
            f"Failed to fetch {dbg} - response was not json. "
            f"Is '{resp.url}' the correct endpoint? - "
            f"Content:\n{resp.content[:1000]}"
        )
    return data


class OIDC:
    """Handle authentication with Azul api."""

    def __init__(self, cfg: config.Config) -> None:
        self.cfg = cfg
        self._oidc_info = None
        # Requests used to force retries on status codes [500, 502, 503, 504]
        verify = True
        if not self.cfg.azul_verify_ssl:
            print("NO VERIFY SSL", file=sys.stderr)
            verify = False
        # bandit has bug that doesn't detect the timeout being set
        self._c = httpx.Client(  # nosec B113
            mounts={
                "http://": httpx.HTTPTransport(retries=5),
                "https://": httpx.HTTPTransport(retries=5),
            },
            timeout=httpx.Timeout(timeout=self.cfg.max_timeout),
            verify=verify,
        )
        # bandit has bug that doesn't detect the timeout being set
        self._local_client = httpx.Client(  # nosec B113
            timeout=httpx.Timeout(timeout=self.cfg.max_timeout),
        )

    def _get_oidc_info(self):
        """Return oidc json document containing locations of required resources for auth."""
        if not self._oidc_info:
            self._fetch_well_known()
        return self._oidc_info

    def _get_authorization_endpoint(self):
        return self._get_oidc_info()["authorization_endpoint"]

    def _get_token_endpoint(self):
        return self._get_oidc_info()["token_endpoint"]

    def _fetch_well_known(self):
        resp = self._local_client.get(self.cfg.oidc_url, timeout=httpx.Timeout(timeout=self.cfg.oidc_timeout))
        self._oidc_info = _get_json(resp, "well known")

    def get_client(self):
        """Return a httpx client object with an up to date authorization token."""
        self._c.headers.update({"authorization": "Bearer " + self.get_access_token()})
        return self._c

    def _via_service_token(self):
        """Retrieve a token from OIDC provider using service account flow."""
        resp = self._local_client.post(
            self._get_token_endpoint(),
            data={
                "response_type": "token",
                "client_id": self.cfg.auth_client_id,
                "client_secret": os.environ.get("AZUL_CLIENT_SECRET") or self.cfg.auth_client_secret,
                "grant_type": "client_credentials",
                "scope": self.cfg.auth_scopes,
            },
            timeout=httpx.Timeout(timeout=self.cfg.oidc_timeout),
        )
        return _get_json(resp, "via service token")

    def _via_code_callback(self):
        """Retrieve a token from OIDC provider using code callback."""
        # prove that requestor of token is same as receiver with code challenge and verify
        code_verifier = base64.urlsafe_b64encode(os.urandom(40)).decode("utf-8")
        code_verifier = re.sub("[^a-zA-Z0-9]+", "", code_verifier)

        code_challenge = hashlib.sha256(code_verifier.encode("utf-8")).digest()
        code_challenge = base64.urlsafe_b64encode(code_challenge).decode("utf-8")
        code_challenge = code_challenge.replace("=", "")

        port = 8080
        callback_url = f"http://localhost:{port}/client/callback"

        state = str(random.randint(1000000, 9999999))  # nosec B311
        params = {
            "response_type": "code",
            "client_id": self.cfg.auth_client_id,
            "redirect_uri": callback_url,
            "state": state,
            "scope": self.cfg.auth_scopes,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }
        url_ask_user = self._get_authorization_endpoint() + "?" + urllib.parse.urlencode(params)
        print(f"Please navigate to the following url to continue authentication:\n{url_ask_user}", file=sys.stderr)
        code = callback.receive_code(state, path="/client/callback", hostname="localhost", port=port)
        if not code:
            raise Exception("No token retrieval code was returned")

        resp = self._local_client.post(
            self._get_token_endpoint(),
            data={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": self.cfg.auth_client_id,
                "state": state,
                "scope": self.cfg.auth_scopes,
                "redirect_uri": callback_url,
                "code_verifier": code_verifier,
            },
            timeout=httpx.Timeout(timeout=self.cfg.oidc_timeout),
        )
        return _get_json(resp, "via code callback")

    def _via_refresh(self, tk: dict):
        """Obtain new tokens using previously issued refresh token."""
        refresh_token = tk.get("refresh_token", None)
        # Full re-auth if there is no refresh token
        if not refresh_token:
            return self._get_token_non_refresh()
        # need to refresh the current token
        resp = self._local_client.post(
            self._get_token_endpoint(),
            data={
                "grant_type": "refresh_token",
                "client_id": self.cfg.auth_client_id,
                "refresh_token": refresh_token,
                "scope": self.cfg.auth_scopes,
            },
            timeout=httpx.Timeout(timeout=self.cfg.oidc_timeout),
        )
        if 400 <= resp.status_code < 500:
            # maybe refresh token has expired, retry full auth
            return None
        return _get_json(resp, "via refresh")

    def _get_token_non_refresh(self):
        # use auth method nominated in config
        atype = self.cfg.auth_type
        if atype == "none":
            # no access token is required
            tk = {}
        elif atype == "callback":
            tk = self._via_code_callback()
        elif atype == "service":
            tk = self._via_service_token()
        else:
            raise NotImplementedError(atype)
        return tk

    def _get_token(self):
        """Get auth tokens to Azul."""
        if self.cfg.auth_type == "none":
            # no access token is required
            return {}

        if self.cfg.auth_token:
            if time.time() > self.cfg.auth_token_time + 60:
                # refresh the token
                tk = self._via_refresh(self.cfg.auth_token)
                if not tk:
                    # maybe refresh token has expired
                    print("Warning - Refresh token likely has expired.", file=sys.stderr)
                    tk = self._get_token_non_refresh()

            elif time.time() < self.cfg.auth_token_time + 60:
                # return current token
                return self.cfg.auth_token
        else:
            tk = self._get_token_non_refresh()

        self.cfg.auth_token = tk
        self.cfg.auth_token_time = int(time.time())
        # save updated token to config file
        self.cfg.save()
        return tk

    def get_access_token(self) -> str:
        """Get valid access token to authenticate with Azul."""
        token = self._get_token().get("access_token", "none")
        if not token and self.cfg.auth_type != "none":
            raise Exception("Token required but was not found")
        return token
