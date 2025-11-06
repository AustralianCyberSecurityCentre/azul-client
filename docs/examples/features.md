# Features

Shows how to list and count features stored in Azul for a particular plugin.

```python
# List and count features in azul for the entropy plugin.
from azul_client import api
from azul_client.config import Config, get_config, switch_section

# Setup client
config = get_config()
client = api.Api(config)
feats = client.features.find_features(author="Entropy")

# Print all the features for the entropy plugin
print("Features:")
last_feat_name = ""
for f in feats.items:
    print(f.model_dump_json(indent=2))
    last_feat_name = f.name

# Print number of values for feature.
count_unique_fv = client.features.count_unique_values_in_feature([last_feat_name])
for feat_name, count_ref in count_unique_fv.items():
    print(f"{feat_name}:")
    print(f"name: {count_ref.name}, number of values: {count_ref.values}, number of entities with feature: {count_ref.entities}")

```
