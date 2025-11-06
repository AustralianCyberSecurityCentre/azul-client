# Get Feature Values of Binary set

Gets values of a specific feature based on a set of binaries determined by a query term. 

Example usage to get all `entropy` values where the binaries are in the `testing` source:

```python
from azul_client.api import Api

query_term = "source.name:testing"
input_feature = "entropy"

# initialise API instance
azul_api = Api()

res = azul_api.binaries_meta.find(term=query_term, count_entities=True)
if int(res.items_count) > len(res.items) and int(res.items_count) < 1000:
    # requery with res.items_count as the max
    res = azul_api.binaries_meta.find(term=query_term, max_entities=int(res.items_count), count_entities=True)

hashes = [item.sha256 for item in res.items]
print(hashes)

feature_values = set()
for h in hashes:
    # find the value for the input feature
    meta = azul_api.binaries_meta.get_meta(sha256=h)
    for feat in meta.features:
        if feat.name == input_feature:
            feature_values.add(feat.value)

print(feature_values)
```
