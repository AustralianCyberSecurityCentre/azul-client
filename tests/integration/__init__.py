"""Seed data into Azul to ensure it has the expected binaries already in it before starting any test queries."""

import io
import random
import string
import time

from azul_client import config
from azul_client.api import Api

__all__ = ["module_sha256s", "module_source", "module_ref"]


module_sha256s: list[str] = []
module_source: str = "testing"
module_ref: dict = {"user": "azul-client-test"}
# must provide a valid security string to server during file uploads
upload_security: str = "OFFICIAL"


def setUpModule():
    """Upload all of the assumed to be present files to azul and then wait until they are queryable in azul.

    Max wait is 100 seconds so if it takes
    """
    print("setting up module")
    api = Api(config.get_config())
    temp_kwargs = {
        "source_id": module_source,
        "security": upload_security,
        "references": module_ref,
        "refresh": True,  # Make sure parent binaries are in the system ASAP so we can use them for tests
    }
    number_of_binaries = 10  # Number of binaries to generate
    for i in range(number_of_binaries):
        # Random content to make each test run unique.
        random_string = "".join(random.choices(string.ascii_letters + string.digits, k=500))
        random_content = random_string.encode()
        # x 100 to get size
        std_content = io.BytesIO(
            b"content of generic files uploaded to azul for client tests at least 4096 bytes " * 100
            + b"worth for ssdeep and tlsh etc"
            + bytes("-postfix-" + str(i), "utf-8")
            + random_content
        )

        temp_kwargs["filename"] = f"test_basic_upload-{i}.txt"
        entity = api.binaries_data.upload(std_content, **temp_kwargs)
        module_sha256s.append(entity.sha256)
    print(f"Module file hashes: {module_sha256s}")  # run test with `pytest -s` to see this print out

    start_time = time.time()
    max_wait = 100  # wait up to 100 seconds for binaries to be in the system.
    all_binaries_found = False
    while start_time + max_wait > time.time():
        print("Checking if the module hashes are ready to be queried against for tests.")
        missing = False
        for sha in module_sha256s:
            if not api.binaries_meta.check_meta(sha):
                missing = True
            if not api.binaries_data.check_data(sha):
                missing = True
        if not missing:
            # No binaries are missing they're all queryable in azul now so stop waiting.
            all_binaries_found = True
            break
        time.sleep(2)

    if not all_binaries_found:
        raise Exception(
            f"Failed to verify all files were uploaded to azul in {max_wait} seconds.\n !!! - Tests cannot run as module level seeding failed."
        )
