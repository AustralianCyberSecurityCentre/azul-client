# Bulk Download

Downloads all binaries which have been uploaded today.

```python
from azul_client import Api

import pendulum

# initialize API instance
azul_api = Api()

# get today's date in string format
today = pendulum.today().format("YYYY-MM-DD")

# build a query string
query = f"timestamp:\"{today}\""

# find binaries in Azul based on query string
res = azul_api.binaries_meta.find(query, max_entities=10)

# get all the hashes
hashes = [item.sha256 for item in res.items]

# download all hashes in a zip
if hashes:
    content = azul_api.binaries_data.download_bulk(hashes)
    if content:
        with open("samples.zip", "wb") as f:
            f.write(content)
```
