# Binary Upload

Uploads a binary and a child binary to Azul and then checks the binaries metadata.

```python
import tempfile
from hashlib import sha256

import pendulum

from azul_client import api, config

# Setup client
config = config.get_config()
client = api.Api(config)

with tempfile.NamedTemporaryFile() as f:
    f.write(b"Dummy test files to upload to azul abcdef")
    f.flush()

    # Setup default source information for upload
    source_dict = client.sources.get_all_sources()
    upload_refs = dict()
    upload_source_id = None

    preferred_target_source = "testing"
    # Choose and upload to first available source (Ideally you'd pick a source)
    for source_name, source_detail in source_dict.items():
        # Prefer testing source if possible.
        if source_name in source_dict.keys() and source_name != preferred_target_source:
            continue

        upload_source_id = source_name
        for ref in source_detail.references:
            if ref.required:
                upload_refs[ref.name] = "Uploaded by dummy test script to this source."

    # Set first security value
    security_presets = client.security.get_security_settings().get("presets", [])
    if len(security_presets) == 0:
        raise Exception("No preset security strings to upload with")

    print(f"Security presets are (ensure you can use the selected default): {security_presets}")

    print(
        f"Uploading to source {upload_source_id}, with references {upload_refs}, and security string {security_presets[-1]}"
    )

    # Upload a file directly to a source
    result = client.binaries_data.upload(
        f.name,
        source_id=upload_source_id,
        references=upload_refs,
        filename="dummy_testing_file.txt",
        timestamp=pendulum.now(pendulum.UTC).to_iso8601_string(),
        security=security_presets[-1],
        refresh=True,  # Make it available immediately so we can add a child binary
    )
    print(f"successfully uploaded test file with sha256 {result.sha256}")

    # Upload a child file to the parent
    child_upload = client.binaries_data.upload_child(
        file_path_or_contents=b"Dummy child file!",
        parent_sha256=result.sha256,
        relationship={"action": "derived"},
        filename="dummy_testing_file.txt",
        timestamp=pendulum.now(pendulum.UTC).to_iso8601_string(),
        security=security_presets[-1],
    )

    print(
        f"successfully uploaded child test file with sha256 {child_upload.sha256} and attached to parent {result.sha256}"
    )
    print("NOTE - because refresh was not true for child binary it will take longer to appear on the UI!")


uploaded_binary_sha256 = result.sha256
print(
    f"Does the uploaded binary have metadata: 'yes' if {client.binaries_meta.check_meta(uploaded_binary_sha256)} else 'no'"
)
basic_metadata = client.binaries_meta.get_meta(uploaded_binary_sha256)
print(f"Binary has {len(basic_metadata.features)} features.")
print(f"Binary has security set to '{basic_metadata.security}'")

if len(basic_metadata.features) > 0:
    print(f"First feature for binary is: {basic_metadata.features[0].model_dump_json(indent=2)}")
```
