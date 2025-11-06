import os
from unittest import TestCase

import httpx
import respx
from azul_bedrock import models_restapi
from azul_bedrock import models_settings as azs
from click.testing import CliRunner

from azul_client import Api, client
from azul_client import config as mconfig


class ClientTestOther(TestCase):
    def setUp(self):
        mconfig.location = ""
        cur_api = Api(
            mconfig.Config(
                auth_type="none",
                oidc_url="http://localhost",
                auth_client_id="servicer",
            )
        )
        client.api = cur_api
        self.api = cur_api

        self.cli = CliRunner()

        return super().setUp()

    @respx.mock
    def test_security(self):
        """Test listing out security works as expected"""
        security_resp = {
            "extra": {"extra extra": "read all about it"},
            "presets": [
                "OFFICIAL",
                "OFFICIAL TLP:CLEAR",
                "OFFICIAL TLP:GREEN",
                "MORE OFFICIAL FIREBALL REL:APPLE",
            ],
        }
        # Mock security response
        respx.get(f"{self.api.config.azul_url}/api/v0/security").mock(
            httpx.Response(status_code=200, json=security_resp)
        )

        # Standard security listing
        res = self.cli.invoke(client.security)
        self.assertIn("OFFICIAL", res.stdout)
        self.assertIn("OFFICIAL TLP:CLEAR", res.stdout)
        self.assertIn("OFFICIAL TLP:GREEN", res.stdout)
        self.assertIn("MORE OFFICIAL FIREBALL REL:APPLE", res.stdout)
        self.assertNotIn("extra", res.stdout)

        # Try with full option
        res = self.cli.invoke(client.security, ["--full"])
        self.assertIn("OFFICIAL", res.stdout)
        self.assertIn("OFFICIAL TLP:CLEAR", res.stdout)
        self.assertIn("OFFICIAL TLP:GREEN", res.stdout)
        self.assertIn("MORE OFFICIAL FIREBALL REL:APPLE", res.stdout)
        self.assertIn("extra", res.stdout)

    @respx.mock
    def test_list_sources(self):
        """Test that sources list out as expected."""
        source_resp = {
            "testing": azs.Source(
                references=[
                    azs.Source.SourceReference(
                        name="custom_ref_1",
                        required=True,
                        description="testing description 1",
                    )
                ]
            ).model_dump(),
            "other": azs.Source(
                references=[
                    azs.Source.SourceReference(
                        name="custom_ref_2",
                        required=False,
                        description="other reference field",
                    )
                ]
            ).model_dump(),
        }
        respx.get(f"{self.api.config.azul_url}/api/v0/sources").mock(
            httpx.Response(status_code=200, json={"data": source_resp})
        )

        # Listing just the source name works as expected.
        res = self.cli.invoke(client.sources_list)
        # For debugging issues.
        print(res)

        self.assertIn("testing", res.stdout)
        self.assertIn("other", res.stdout)
        self.assertNotIn("custom_ref_1", res.stdout)
        self.assertNotIn("custom_ref_2", res.stdout)

        # List will all the information about all sources.
        res = self.cli.invoke(client.sources_full)
        print(res)
        self.assertIn("testing", res.stdout)
        self.assertIn("other", res.stdout)
        self.assertIn("custom_ref_1", res.stdout)
        self.assertIn("custom_ref_2", res.stdout)

        # Test case of listing a specific source
        res = self.cli.invoke(client.sources_info, ["testing"])
        print(res)
        self.assertIn("testing", res.stdout)
        self.assertIn("custom_ref_1", res.stdout)
        self.assertIn("testing description 1", res.stdout)

        self.assertNotIn("other", res.stdout)
        self.assertNotIn("custom_ref_2", res.stdout)

    @respx.mock
    def test_list_plugins(self):
        mock_resp: list[models_restapi.LatestPluginWithVersions] = [
            models_restapi.LatestPluginWithVersions(
                versions=["2023", "2024", "2025"],
                newest_version=models_restapi.PluginEntity(
                    name="dummy",
                    version="2025",
                    description="plugin description",
                    security="OFFICIAL",
                    category="plugin",
                ),
            ).model_dump(),
        ]
        respx.get(f"{self.api.config.azul_url}/api/v0/plugins").mock(
            httpx.Response(status_code=200, json={"data": mock_resp})
        )
        mock_single_resp = models_restapi.PluginInfo(
            num_entities=10,
            plugin=models_restapi.PluginEntity(
                name="dummy",
                version="2023",
                description="different desc so I can verify",
                security="OFFICIAL",
                category="plugin",
            ),
            status=[],
        ).model_dump()
        respx.get(f"{self.api.config.azul_url}/api/v0/plugins/dummy/versions/2023").mock(
            httpx.Response(status_code=200, json={"data": mock_single_resp})
        )

        # List all plugins
        res = self.cli.invoke(client.plugins_list)
        print(res.stdout)
        self.assertIn("dummy 2025", res.stdout)

        # Get detail about latest version of this plugin.
        res = self.cli.invoke(client.plugin_info, ["dummy"])
        self.assertIn("dummy", res.stdout)
        self.assertIn("2025", res.stdout)
        self.assertIn("plugin description", res.stdout)
        self.assertIn("OFFICIAL", res.stdout)

        # Get detail about older version of this plugin.
        res = self.cli.invoke(client.plugin_info, ["dummy", "--version", "2023"])
        print(res)
        self.assertIn("dummy", res.stdout)
        self.assertIn("2023", res.stdout)
        self.assertIn("different desc so I can verify", res.stdout)
        self.assertIn("OFFICIAL", res.stdout)
