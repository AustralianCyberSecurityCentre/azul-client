# Configuration Setup

Different ways of setting up the Azul OIDC to authenticate to Azul so the client can be used.

Also shows how to authenticate if you are using a service account.

```python
# Process of setting up access to the user client.
from azul_client import api
from azul_client.config import Config, get_config, switch_section

# --- BASIC USER EXAMPLE
# Create a config for a basic the user
print("--- Create new Config and get prompted for callback ---")
user_config = Config(
    azul_url="https://<azul-url>/",
    oidc_url="https://<oidc-url>",
    auth_type="callback",
    auth_scopes="read openid profile offline_access",
    auth_client_id="<client-id>",
    azul_verify_ssl=True,
)
# NOTE - you will be prompted with the callback URL for auth using this method.

# Use the config to list all the sources to verify it worked.
client = api.Api(user_config)
source_dict = client.sources.get_all_sources()

# Save the configuration to ~/.azul.ini for future use if you want to use the CLI for example.
user_config.save()

# --- Using a saved configuration
# Using the saved configuration (no prompt required)
print("--- load saved configuration ---")
config_loaded = get_config()
client2 = api.Api(config_loaded)
# List first 3 plugins and print their name version and config.
plugin_list = client2.plugins.get_all_plugins()
for plugin in plugin_list[:3]:
    print(plugin.newest_version.name, plugin.newest_version.version)
    print(plugin.newest_version.config)

# Switch the section and save the config to a different section (useful if you have multiple sections in your configuration)
switch_section("newsection")
config_loaded.save()

# --- Create service account Config (no prompt)
print("--- service config ---")
service_config = Config(
    azul_url="https://<azul-url>/",
    oidc_url="https://<oidc-url>",
    auth_type="service",
    auth_scopes="openid profile email offline_access",
    auth_client_id="<client-id>",
    auth_client_secret="<secret-value>",
    azul_verify_ssl=True,
)
client3 = api.Api(service_config)
my_opensearch_details = client3.users.get_opensearch_user_info()
print(f"Am I privileged {'yes' if my_opensearch_details.privileged else 'no'}")
print("opensearch roles:\n", my_opensearch_details.roles)

# Save the service config for later use,
# NOTE - switch_section is global, so every time you switch it all subsequent azul-client calls will use that section.
switch_section("service")
service_config.save()
```
