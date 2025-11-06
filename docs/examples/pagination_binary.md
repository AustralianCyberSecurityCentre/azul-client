# Pagination

Paginate over a large number of binaries and select some to download when they have content.

Also download and optionally uncart binaries.

```python
# Look at lots of binaries in azul by iterating over them with queries.
# Select specific binaries to download and uncart.
import io

import cart

from azul_client import api
from azul_client.api.binaries_meta import FindOptions
from azul_client.config import Config, get_config, switch_section

# Setup client
config = get_config()
client = api.Api(config)

# Find the first 2k files less than 1kB
index = 0
found_binaries = client.binaries_meta.find_all(find_options=FindOptions(less_than_size_bytes=1024), max_binaries=2000)
print(f"Expected total binaries is {found_binaries.approx_total}")
for binary in found_binaries.iter:
    index += 1
    print(f"found {index}/{found_binaries.approx_total}")
    print(f"found hash {binary.sha256}")

# Iterate over the 1kB files that are less than 1kB and text files.
for binary in client.binaries_meta.find_all(
    find_options=FindOptions(less_than_size_bytes=1024, file_formats="text/plain")
):
    index += 1
    print(f"found hash {binary.sha256}")
    if client.binaries_data.check_data(binary.sha256):
        print(f"found hash {binary.sha256} has binary data!")
        break

# Download last binary file
print("download cart")
cart_of_file = client.binaries_data.download(binary.sha256)
with open("downloaded.cart", "wb") as f:
    f.write(cart_of_file)


# Uncart the downloaded cart. (not recommended unless you trust the file)
"""
print("extract cart")
cart.unpack_file("downloaded.cart", "downloaded.txt")
"""

# Uncart directly from download
"""
print("download to raw")
cart_of_file = client.binaries_data.download(binary.sha256)
with open("downloaded_raw.txt", "wb") as f:
    cart.unpack_stream(io.BytesIO(cart_of_file), f)
"""
```
