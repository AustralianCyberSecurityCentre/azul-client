# Azul Client

Azul client is a near complete client for Azul's RestAPI.

Interact with Azul using your terminal instead of clicking in the UI a thousand times!

Tested on ubuntu 22.04.

## Install

`pip install azul-client`

## Setup

Azul Client requires a config file located at ~/.azul.ini

A default config will be generated on first run.

You will need to adjust the config options as appropriate.

```yaml
[default]
azul_url = http://localhost
oidc_url = http://keycloak/.well-known/openid-configuration
auth_type = callback
auth_scopes =
auth_client_id = azul-web
auth_client_secret =
azul_verify_ssl = True
auth_token = {}
auth_token_time = 0
max_timeout = 300.0
oidc_timeout = 10.0
```

### Root CA

If you have extra Root CAs, you will need to make httpx aware of them or it will complain.

Ubuntu - `export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt`

Red Hat - `export SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt`

Alternatively you can point to a certificate directory

`export SSL_CERT_DIR=/etc/ssl/certs`

This can be added to your ~/.bashrc to prevent you from having to do it for every terminal session.

## Usage

For usage guidance refer to the [API](./docs/api.md) and [CLI](./docs/cli.md) documentation.

## Integration test suite

The integration test suite is in the tests/integration folder.

The `setUpModule` method in the file `tests/integration/__init__.py` creates all files in azul that need to be available for querying and uploading child/dataless.
It also waits for those uploaded files to be available in Azul which means during tests you can assume those files exist.

It also exports the sha256's of the files it uploaded to ensure the tests can import those sha256's for their testing.

NOTE - the first time you run the test suite particularly if you've added new files to the module it may be slow. But all subsequent runs will be much faster.
