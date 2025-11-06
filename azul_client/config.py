"""Config handling."""

import configparser
import json
import os
import sys

import click
import pydantic
from pydantic_settings import BaseSettings

config_section = "default"


def switch_section(section: str):
    """Switch to a different azul deployment configured in the ini."""
    global config_section
    config_section = section


@click.group(name="config")
def _client_config():
    """Change azul-client configuration."""
    pass


class ConfigLocation(BaseSettings):
    """Path to the azul.ini settings."""

    azul_config_location: str = os.path.join(os.path.expanduser("~"), ".azul.ini")


class Config(BaseSettings):
    """Config wrapper."""

    azul_url: str = "http://localhost"
    oidc_url: str = "http://keycloak/.well-known/openid-configuration"
    auth_type: str = "callback"
    auth_scopes: str = ""
    auth_client_id: str = "azul-web"
    auth_client_secret: str = ""  # nosec B105
    azul_verify_ssl: bool = True
    auth_token: dict | None = {}
    auth_token_time: int = 0
    max_timeout: float = 300.0
    oidc_timeout: float = 10.0

    @pydantic.field_validator("azul_url")
    def no_trailing_slash(cls, v):
        """Remove trailing slash from azul_url."""
        return v.rstrip("/")

    def save(self):
        """Save the current configuration."""
        tmp = self.model_dump()
        # save auth token as json string
        tmp["auth_token"] = json.dumps(tmp["auth_token"])

        location = ConfigLocation().azul_config_location
        cfg = configparser.ConfigParser()
        cfg.read(location)
        cfg[config_section] = tmp
        with open(ConfigLocation().azul_config_location, "w") as configfile:
            cfg.write(configfile)


@_client_config.command()
def clear_auth():
    """Reset current auth information."""
    conf = get_config()
    conf.auth_token = ""  # nosec B105
    conf.auth_token_time = 0
    conf.save()


def get_config():
    """Get config loaded from file."""
    location = ConfigLocation().azul_config_location
    if not os.path.exists(location):
        print(f"ERROR - no config found - generating default at {location}", file=sys.stderr)
        print("You will likely need to edit this config.", file=sys.stderr)
        conf = Config()
        conf.save()

    print(f"Loading config [{config_section}] from {location}", file=sys.stderr)
    tmp = configparser.ConfigParser()
    tmp.read(location)
    conf = {}
    try:
        # configparser has an odd data structure, convert to dictionary
        conf = {**tmp[config_section]}
    except KeyError:
        if config_section == "default":
            print(f"Config section [{config_section}] is invalid, generating defaults", file=sys.stderr)
        else:
            raise Exception(f"config section [{config_section}] is invalid")

    # the auth token was saved as a json string
    if conf.get("auth_token"):
        conf["auth_token"] = json.loads(conf["auth_token"])
    config = Config(**conf)
    print(f"using Azul API at {config.azul_url}\n", file=sys.stderr)
    return config
