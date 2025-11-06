# Get Hashes

Downloads all binaries that are `code/html` and saves the sha256's of the files to a text file.

```python
"""Script to demonstrate retrieving 'all' binaries matching the specified criteria.

Hashes will be downloaded to hashes.txt alongside this script.
"""

import os
import time

from azul_client import config
from azul_client.api import Api
from azul_client.api.binaries_meta import FindOptions

# the properties to search for across binaries
find_options = FindOptions(file_formats="code/html")


def main():
    """Main script."""
    api = Api(config.get_config())
    fetched = 0
    started = time.time()
    dir = os.path.dirname(__file__)
    print(f"starting find of '{find_options.to_query()}'")
    try:
        with open(os.path.join(dir, "hashes.txt"), "w") as f:
            find_result = api.binaries_meta.find_all(find_options, max_binaries=0)
            for binary in find_result:
                f.write(f"{binary.sha256}\n")
                fetched += 1
                if fetched % 5000 == 0:
                    print(
                        f"found {fetched} of approx {find_result.approx_total}"
                        f" {fetched / find_result.approx_total * 100:.1f}% in {time.time() - started:.2f}s"
                    )
    except KeyboardInterrupt:
        print("early termination")
    print(
        f"found {fetched} of approx {find_result.approx_total}"
        f" {fetched / find_result.approx_total * 100:.1f}% in {time.time() - started:.2f}s"
    )


if __name__ == "__main__":
    main()

```
