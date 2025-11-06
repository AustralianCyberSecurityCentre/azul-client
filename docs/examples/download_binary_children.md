# Download Binary Children

Downloads all the child binaries given a parent binary

```python
from azul_client.api import Api

parent_hash = "<insert-parent-sha256-here>"

# initialise API instance
azul_api = Api()

child_hashes = []

# check if meta exists
if azul_api.binaries_meta.check_meta(parent_hash):
    # use API to get metadata for hash
    meta = azul_api.binaries_meta.get_meta(parent_hash)

    # get all of the child hashes from metadata results
    child_hashes = [child.sha256 for child in meta.children]

# enumerate all the child hashes and download
for hash in child_hashes:
    # download the actual binary data (cart'ed)
    content = azul_api.binaries_data.download(hash)
    if content:
        # write the binary to a file
        with open(f"{hash}.cart", "wb") as f:
            f.write(content)
```
