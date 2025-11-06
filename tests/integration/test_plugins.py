from azul_bedrock import models_restapi

from azul_client.exceptions import BadResponse, BadResponse404

from .base_test import BaseApiTest


class TestPluginsApi(BaseApiTest):
    def setUp(self):
        super().setUp()

        # Ensure this is unset between runs
        self.api.set_excluded_security(None)

    @staticmethod
    def convert_plugins_to_dict(
        list_of_plugins: list[models_restapi.LatestPluginWithVersions],
    ) -> dict[str, models_restapi.PluginEntity]:
        """Helper functions to convert the list of plugins into a dictionary with plugin name as the key."""
        result = dict()
        for p in list_of_plugins:
            result[p.newest_version.name] = p.newest_version
        return result

    def test_get_all_plugins(self):
        """Verify that you can get a large list of the plugins installed and running on azul."""
        list_of_plugins = self.api.plugins.get_all_plugins()
        self.assertGreater(len(list_of_plugins), 10)
        self.assertGreaterEqual(len(list_of_plugins[0].versions), 1)
        self.assertIsInstance(list_of_plugins[0].newest_version, models_restapi.PluginEntity)

        dict_of_plugins = self.convert_plugins_to_dict(list_of_plugins)
        self.assertIn(self.entropy_plugin_name, dict_of_plugins.keys())
        self.assertIn(self.python_known_plugin_name, dict_of_plugins.keys())

    def test_security_exclusion_get_all_plugin(self):
        self.api.set_excluded_security([self.min_security])
        plugin_list_min = self.api.plugins.get_all_plugins()

        self.api.set_excluded_security([self.max_security])
        plugin_list_max = self.api.plugins.get_all_plugins()

        plugin_list_default = []
        try:
            self.api.set_excluded_security([self.default_security])
            plugin_list_default = self.api.plugins.get_all_plugins()
        except BadResponse404:
            pass

        # These should be large (all the official plugins minus maybe 1 or 2).
        self.assertGreater(len(plugin_list_min), 0)
        self.assertGreater(len(plugin_list_max), 0)
        # This may be 0 or a few
        self.assertGreaterEqual(len(plugin_list_default), 0)

        # when we cut out the default plugins that should remove a lot of plugins
        # So these numbers can't be equal and the default list may even be length 0.
        self.assertGreater(len(plugin_list_max), len(plugin_list_default))
        self.assertGreater(len(plugin_list_min), len(plugin_list_default))

    def test_get_all_plugin_statuses(self):
        """Test that you can get all the plugin statuses and that entropy has completed at least 100 jobs."""
        response_model = self.api.plugins.get_all_plugin_statuses()
        self.assertGreater(len(response_model), 10)

        checked_entropy = False
        for model in response_model:
            if model.newest_version.name == self.entropy_plugin_name:
                checked_entropy = True
                self.assertIsNotNone(model.last_completion)
                self.assertGreater(model.success_count, 100)

        self.assertTrue(checked_entropy, "Failed to find a status for entropy!")

    def test_get_plugin(self):
        """Verify that when you get plugins you can get their configurations or not if they don't have them."""
        list_of_plugins = self.api.plugins.get_all_plugins()
        dict_of_plugins = self.convert_plugins_to_dict(list_of_plugins)
        latest_version = dict_of_plugins[self.entropy_plugin_name].version
        plugin_info = self.api.plugins.get_plugin(self.entropy_plugin_name, latest_version)
        self.assertGreater(plugin_info.num_entities, 1)
        self.assertIsInstance(plugin_info.plugin.config, dict)
        self.assertGreaterEqual(len(plugin_info.status), 1)

        python_latest_version = dict_of_plugins[self.python_known_plugin_name].version
        plugin_info = self.api.plugins.get_plugin(self.python_known_plugin_name, python_latest_version)
        self.assertGreater(plugin_info.num_entities, 1)
        self.assertIsInstance(plugin_info.plugin.config, dict)
        self.assertGreaterEqual(len(plugin_info.status), 1)
