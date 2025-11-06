# Querying binaries which have a specific feature value

Finds the number of binaries which have a specific base64 alphabet

```python
from azul_client import Api


from azul_bedrock import models_restapi

# initialise API instance
azul_api = Api()

# query features for b64 alphabet values
res = azul_api.features.find_values_in_feature("b64_alphabet")

# enumerate results for feature values
b64_alphabets = [item.value for item in res.values]
print("All Alphabet values:")
print(b64_alphabets)

# build the model for the query params
feature_value_query = models_restapi.ValueCountItem(name="b64_alphabet", value=b64_alphabets[0])

# query Azul
res = azul_api.features.count_unique_entities_in_featurevalues([feature_value_query])

print("\nCounts for specific value:")
print(res)
```
