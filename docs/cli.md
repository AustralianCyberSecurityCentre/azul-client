# Azul-Client CLI

The azul client CLI has some simple functionality that is intended for basic debugging and simple use.

To view a list of commands you can use the command `azul --help`

For subsequent commands you can drill down with help to see all the subcommands and parameters using help again.

e.g:

```bash
azul b --help
auzl binary put --help
azul sources --help
auzl sources list --help
```

## Using different Configurations

To use different azul configuration defined in your .azul.ini file use the `-c` switch as shown below:

```bash
# Target default config of azul.
azul check <sha256>
# Target locally hosted azul.
azul -c local  check <sha256>
```

```config
[default]
azul_url = https://<url-1>
oidc_url = <oidc-url>
auth_type = callback
auth_scopes = read openid profile offline_access
auth_client_id = <unique-id>
auth_client_secret =
azul_verify_ssl = True
auth_token = {}
auth_token_time = 1747178289
max_timeout = 300.0
oidc_timeout = 10.0
  
[local]
azul_url = http://localhost:8080
oidc_url = <oidc-url>
auth_type = callback
auth_scopes = read openid profile offline_access
auth_client_id = <unique-id>
auth_client_secret =
azul_verify_ssl = True
auth_token = {}
auth_token_time = 1747181848
max_timeout = 300.0
oidc_timeout = 10.0
```

## Azul Details

Before submitting binaries to Azul you typically need to get information required for the submission.

This includes things like the security label you want to use and the source you want to submit to.

To get these you can use the following CLI commands

```bash
# Get the security you want to submit with
# List the preset security strings (usually this is enough)
azul security 
# List all security configuration (useful if you want to set custom security)
azul security --full

# get the source you want to submit to
# List all available sources.
azul sources list
# Detail the `testing` source so you can see all the reference fields
azul sources info testing
# Lists out all the detail about all sources in case the above information is insufficient.
azul sources full
```

## Uploading to Azul

To upload to azul you can either upload an individual file or a whole directory.

Individual file is done like this:

```bash
# Upload the binary in the current directory called 'test.txt' to the source 'samples'
azul binaries put --ref 'description:long description of uploaded file' --ref team:best --timestamp '2025-05-27T02:44:00.000Z' --security OFFICIAL test.txt samples
# Password protected zip
azul binaries put --ref 'description:long description of uploaded file' --ref team:best --timestamp '2025-05-27T02:44:00.000Z' --security OFFICIAL test.zip samples --extract --extract-password infected
```

A whole directory:

```bash
# Upload all the binaries in the folder 'test_folder' to the source 'samples'
azul binaries put --ref 'description:long description of uploaded file' --ref team:best --timestamp '2025-05-27T02:44:00.000Z' --security OFFICIAL test_folder samples
```

If you want to upload a child binary and attach it to an existing binary you can do so as follows
(this is typically done to link two binaries when an analyst has manually extracted something out of the parent that Azul hasn't):

```bash
# Where the parent is a binary already in azul
azul b put-child test.txt --relationship action:extracted --relationship relationship:friend --security 'OFFICIAL TLP:CLEAR' --timestamp '2025-05-27T02:44:00.000Z' --parent 7b9175627491b633cb55408a33af3c85777b4704b021c7529abf635289c14117
```

## Searching Azul

If you want to find files in azul that meet certain conditions you can find them using the search query.

Examples of searching azul for files:

```bash
# List the hashes of the first 100 binaries that were most recently submitted to Azul
azul binaries get

# Get the hashes of the first 10 binaries in azul's sha256's, and sort them in reverse source timestamp order.
azul binaries get --sort-by source.timestamp --sort-asc --max 10

# Download carted copies of 10 the most recently added binaries in azul into the folder download_folder
azul binaries get --max 10 --output download_folder

# Get the hashes for the first 10 binaries in azul that are greater than 2 Megabytes.
# Note that the term syntax can be viewed via the UI and suggestions will be provided for the term query there.
azul binaries get --max 10 --term 'size:>2000000'
```
