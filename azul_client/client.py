"""High level library flow."""

import json
import os
import re
import sys
from tempfile import SpooledTemporaryFile

import click
import pendulum
from azul_bedrock import models_restapi
from pendulum.datetime import DateTime
from pendulum.exceptions import ParserError
from pydantic import BaseModel
from rich.console import Console

from azul_client import config
from azul_client.api import Api
from azul_client.config import _client_config
from azul_client.exceptions import BadResponse, BadResponse404
from azul_client.shared import (
    EXTRACT_DESCRIPTION,
    EXTRACT_PASSWORD_DESCRIPTION,
    FULL_INFO,
    NO_CONFIRMATION_PROMPT,
    REFERENCES,
    SECURITY_STRING_DESCRIPTION,
    TIMESTAMP_DESCRIPTION,
    ExamplesCommand,
    with_examples,
)

api: Api


@click.group()
@click.option("-c", default="default", help="switch to a different configured Azul instance.")
def cli(c: str):
    """Interact with the Azul API via CLI tools."""
    global api
    config.switch_section(c)
    api = Api()


@click.group()
def binaries():
    """Upload, download and get metadata associated with binaries."""
    pass


# azul security
@click.command(name="security", cls=ExamplesCommand)
@with_examples(
    "$ azul security",
    "$ azul security --full",
)
@click.option("--full", is_flag=True, show_default=True, default=False, help=FULL_INFO)
def security(full: bool):
    """List Azul security classification settings."""
    settings = api.security.get_security_settings()
    if not full and settings.get("presets"):
        click.echo("Security Presets:")
        presets: list[str] = settings.get("presets", [])
        click.echo("\n".join(presets))
    else:
        click.echo(json.dumps(settings, indent=2))


@click.group(name="sources")
def sources():
    """List and get information about specific sources."""
    pass


# azul sources list
@sources.command(name="list", cls=ExamplesCommand)
@with_examples("$ azul sources list")
def sources_list():
    """List all of the source ids."""
    all_sources = api.sources.get_all_sources()
    click.echo("Source IDS:")
    click.echo("\n".join(all_sources.keys()))


# azul sources full
@sources.command(name="full", cls=ExamplesCommand)
@with_examples("$ azul sources full")
def sources_full():
    """Get the full source information for each source."""
    all_sources = api.sources.get_all_sources()
    all_sources_dumped = {}
    for k, val in all_sources.items():
        all_sources_dumped[k] = val.model_dump()
    click.echo("Sources:")
    click.echo(json.dumps(all_sources_dumped, indent=2))


# azul sources info
@sources.command(name="info", cls=ExamplesCommand)
@with_examples("$ azul sources info")
@click.argument("source")
def sources_info(source: str):
    """Get summary information about a specific SOURCE by source Id."""
    all_sources = api.sources.get_all_sources()
    for source_id, sourceObj in all_sources.items():
        if source_id.lower() == source.lower():
            click.echo(source_id + ":")
            click.echo("Description: " + sourceObj.description)
            click.echo("Submissions Expire After " + sourceObj.expire_events_after)
            click.echo("References:")
            for ref in sourceObj.references:
                click.echo(f"  name: '{ref.name}'")
                click.echo(f"  description: '{ref.description}'")
                click.echo(f"  required: '{ref.required}'\n")
            break


@click.group()
def plugins():
    """List and get information for plugins in Azul."""
    pass


# azul plugins list
@plugins.command(name="list", cls=ExamplesCommand)
@with_examples("$ azul plugins list")
def plugins_list():
    """List all of the plugins registered in Azul."""
    plugin_list = api.plugins.get_all_plugins()
    click.echo("Plugins (name version):")
    for p in plugin_list:
        if not p.newest_version:
            continue
        click.echo(f"{p.newest_version.name} {p.newest_version.version}")


# azul plugins info
@plugins.command(name="info", cls=ExamplesCommand)
@with_examples(
    "$ azul plugins info Cape",
    "$ azul plugins info RtfInfo",
    "$ azul plugins info RetrohuntIngestor",
)
@click.argument("name")
@click.option("--version", type=str, help="version of the plugin to get info for (defaults to newest)")
def plugin_info(name: str, version: str):
    """Get the details of a plugin with the provided plugin name.

    Plugins list can be viewed with $ azul plugins list.

    Please note: Plugin names ARE case-sensitive.
    """
    if version:
        try:
            details = api.plugins.get_plugin(name, version)
        except BadResponse404:
            click.echo(f"Plugin {name} {version} does not exist check the version and name.")
            return
        except BadResponse as e:
            click.echo(f"Plugin {name} could not be found due to error {e.message}.")
            return
        click.echo(f"Providing detail for plugin {name} {version}")
        click.echo(details.plugin.model_dump_json(indent=2))

    try:
        plugin_list = api.plugins.get_all_plugins()
    except BadResponse as e:
        click.echo(f"Plugin {name} could not be found due to error {e.message}.")
        return

    for p in plugin_list:
        if not p.newest_version:
            continue
        if p.newest_version.name == name:
            click.echo(f"Providing detail for plugin {p.newest_version.name} {p.newest_version.version}")
            click.echo(p.newest_version.model_dump_json(indent=2))
            return
    click.echo(f"Plugin {name} could not be found, check the name is valid.")


# azul binaries check
@binaries.command(cls=ExamplesCommand)
@with_examples(
    "$ azul binaries check <SHA256>",
    "$ azul b check <SHA256>",
)
@click.argument("sha256")
def check(sha256: str):
    """Check if the binary metadata for a SHA256 is in Azul or not."""
    if api.binaries_meta.check_meta(sha256):
        click.echo("Binary metadata available")
    else:
        click.echo("Binary metadata NOT available")
        sys.exit(1)


# azul binaries check-data
@binaries.command(cls=ExamplesCommand)
@with_examples(
    "$ azul binaries check-data <SHA256>",
    "$ azul b check-data <SHA256>",
)
@click.argument("sha256")
def check_data(sha256: str):
    """Check if the binary for a SHA256 is in Azul or not."""
    if api.binaries_data.check_data(sha256):
        click.echo("Binary data available")
    else:
        click.echo("Binary data NOT available")
        sys.exit(1)


def _walk_files_in_path(path: str) -> list[str]:
    """Walks a user given path for files."""
    input_files = []
    if os.path.isdir(path):
        for root, dirs, files in os.walk(path, followlinks=False):
            files = [f for f in files if not f[0] == "."]
            dirs[:] = [d for d in dirs if not d[0] == "."]
            for name in files:
                loc = os.path.join(root, name)
                input_files.append(loc)
    elif os.path.isfile(path):
        input_files.append(path)
    else:
        raise Exception("cannot upload something that is not a folder or file")

    return input_files


def _shared_submit(
    confirmed: bool,
    path: str,
    *,
    security: str = "",
    timestamp: str = "",
    extract: bool = False,
    extract_password: str = "",
    parent: str = "",
    parent_rels: dict[str, str] | None = None,
    source: str = "",
    source_refs: dict | None = None,
    from_stdin: bool = False,
):
    """Common class for submitting binaries to Azul."""
    security = security if security else ""
    if not timestamp:
        timestamp = pendulum.now(pendulum.UTC).to_iso8601_string()
    else:
        try:
            timestamp_datetime = pendulum.parse(timestamp)
            if not isinstance(timestamp_datetime, DateTime):
                raise ValueError(f"Provided datetime {timestamp} is not in a valid format.")
            timestamp = timestamp_datetime.to_iso8601_string()
        except ParserError:
            click.echo(f"Error - Unable to parse timestamp: {timestamp}")
            sys.exit(1)

    if not from_stdin:
        raw_input_files = _walk_files_in_path(path)

        # generate azul file names
        input_files = []
        for filepath in raw_input_files:
            # try to remove provided path, unless that was a reference to a specific file
            # in which case keep the filename only
            adjusted = filepath.removeprefix(path)
            filename = os.path.basename(filepath)
            if filename not in adjusted:
                adjusted = filename
            input_files.append((filepath, adjusted))

        # print info and confirm to upload
        click.echo(f"{len(input_files)} files found including:")
        for _, filepath in input_files[:10]:
            click.echo(filepath)
    else:
        # Read file in chunks into a spooled temporary file.
        chunk_size = 1024 * 1024 * 1024
        spooledFile = SpooledTemporaryFile(max_size=chunk_size)
        while chunk := sys.stdin.buffer.read(chunk_size):
            spooledFile.write(chunk)
        file_size = spooledFile.tell()
        click.echo(f"Read {file_size} bytes of binary from stdin")
        spooledFile.seek(0)
        input_files = [(spooledFile, path)]

    if from_stdin:
        click.echo(f"Filename: {path}")
    click.echo(f"Security: {security}")
    click.echo(f"Timestamp: {timestamp}")
    if not from_stdin:
        click.echo(f"Extract: {extract}")
        click.echo(f"Extract Password: {extract_password}")
    if parent:
        click.echo(f"Parent: {parent}")
        click.echo(f"Relationship: {parent_rels}")
    else:
        click.echo(f"Source: {source}")
        click.echo(f"References: {source_refs}")

    # put-stdin can't process confirmation on stdin, instead rerun with -y arg
    if from_stdin and not confirmed:
        click.echo("Rerun command with -y argument to proceed with upload of 1 file")
        sys.exit(1)

    if not confirmed and not click.confirm(f"Proceed with upload of {len(input_files)} files?"):
        sys.exit(1)

    # submit each file
    for fullpath, filepath in input_files:
        with _open_wrapper(fullpath) as f:
            try:
                if parent:
                    if parent_rels is None:
                        raise ValueError(
                            f"{parent_rels=} must be a dictionary with at least one key value pair for uploading a child binary."
                        )
                    resp = api.binaries_data.upload_child(
                        f,
                        parent_sha256=parent,
                        relationship=parent_rels,
                        security=security,
                        filename=filepath,
                        timestamp=timestamp,
                        extract=extract,
                        password=extract_password,
                    )
                else:
                    resp = api.binaries_data.upload(
                        f,
                        security=security,
                        source_id=source,
                        filename=filepath,
                        timestamp=timestamp,
                        references=source_refs,
                        extract=extract,
                        password=extract_password,
                    )
                click.echo(f"{filepath} - {resp.sha256}")

            except BadResponse as e:
                err = json.loads(e.content).get("message")
                if "no content in request body" in err:
                    err = "Submitted file was empty"
                click.echo(f"Error - {err}")


def _open_wrapper(input_source: str | SpooledTemporaryFile):
    if isinstance(input_source, str):
        # if some_path was given, open it
        return open(input_source, "rb")

    if isinstance(input_source, SpooledTemporaryFile):
        print("stdin times!")
        # file input was from stdin, return that instead

        return input_source


def _print_model(model: BaseModel, pretty: bool):
    """Prints a Pydantic model for user consumption, with a pretty filter as required."""
    if pretty:
        # Configure our environment for using a pager if possible
        if "MANPAGER" not in os.environ and "PAGER" not in os.environ:
            os.environ["PAGER"] = "less -r"

        # Guess to see if the pager we are using supports color
        colour_supported = "less -r" in os.environ.get("MANPAGER", "") or "less -r" in os.environ.get("PAGER", "")

        console = Console()
        # Enable a pager for the entity document (its big) if this is an interactive terminal
        if console.is_terminal:
            with console.pager(styles=colour_supported):
                console.print(model)
        else:
            console.print(model)
    else:
        # Dump the entire JSON document for use with e.g. jq
        click.echo(model.model_dump_json(indent=4))


# azul binaries put-child
@binaries.command(cls=ExamplesCommand)
@with_examples(
    '$ azul b put-child payload_exe --parent <SHA256> --security OFFICIAL --relationship "actions:downloads"',
    "",
    '$ azul b put-child payload_exe --parent <SHA256> --security OFFICIAL --relationship "actions:downloads" --timestamp 2021-12-21T11:11:11Z',
    "",
    '$ azul b put-child payload_exe --parent <SHA256> --security OFFICIAL --relationship "actions:downloads" --extract',
    "",
    '$ azul b put-child payload_exe --parent <SHA256> --security OFFICIAL --relationship "actions:downloads" --extract --extract-password infected',
    "",
    '$ azul b put-child payload_exe --parent <SHA256> --security OFFICIAL --relationship "actions:downloads" --relationship "to:C:/<location>" --relationship "from:bad[.]domain/malwares[.]exe"',
)
@click.argument("path")
@click.option("-y", is_flag=True, show_default=True, default=False, help=NO_CONFIRMATION_PROMPT)
@click.option("--timestamp", type=str, help=TIMESTAMP_DESCRIPTION)
@click.option("--security", required=True, type=str, help=SECURITY_STRING_DESCRIPTION)
@click.option("--parent", required=True, type=str, help="SHA256 of parent file")
@click.option(
    "-r",
    "--relationship",
    required=True,
    multiple=True,
    type=str,
    metavar="KEY:VALUE",
    help="""
    Details of the relationship between the child and parent samples.
    All relationships aside from "action" show the supplied key in the UI.
    Multiple relationship flags can be supplied.
    """,
)
@click.option(
    "--extract",
    is_flag=True,
    show_default=True,
    default=False,
    help=EXTRACT_DESCRIPTION,
)
@click.option("--extract-password", type=str, help=EXTRACT_PASSWORD_DESCRIPTION)
def put_child(
    y: bool,
    path: str,
    timestamp: str,
    security: str,
    parent: str,
    relationship: list[str],
    extract: bool,
    extract_password: str,
):
    """Uploads all files in PATH to Azul as CHILD of SHA256.

    PATH is the location of the file/files/directory.

    A PARENT, RELATIONSHIP, and SECURITY are required for all files.
    """
    parsed_relationships = [r.split(":", 1) for r in relationship]
    relation_dict = {item[0]: item[1].lstrip() for item in parsed_relationships}
    _shared_submit(
        y,
        path,
        security=security,
        timestamp=timestamp,
        parent=parent,
        parent_rels=relation_dict,
        extract=extract,
        extract_password=extract_password,
    )


# azul binaries put
@binaries.command(cls=ExamplesCommand)
@with_examples(
    '$ azul b put malware_dll <SOURCE> --security OFFICIAL --ref "description:malware desc"',
    "",
    '$ azul b put samples/ <SOURCE> --security OFFICIAL --ref "description:bulk malware samples"',
    "",
    '$ azul b put malware_dll <SOURCE> --security OFFICIAL --ref "description:malware desc" --timestamp 2012-12-21T11:11:11Z',
    "",
    '$ azul b put malware_dll.zip <SOURCE> --security OFFICIAL --ref "description:malware desc" --extract',
    "",
    '$ azul b put malware_dll.zip <SOURCE> --security OFFICIAL --ref "description:malware desc" --extract-password infected',
    "",
    '$ azul b put malware_dll.zip <SOURCE> --security OFFICIAL --ref "description:malware desc" --extract --extract-password infected --timestamp 2012-12-21T11:11:11Z',
)
@click.argument("path")
@click.argument("source")
@click.option("-y", is_flag=True, show_default=True, default=False, help=NO_CONFIRMATION_PROMPT)
@click.option(
    "--ref",
    type=str,
    multiple=True,
    metavar="KEY:VALUE",
    help=REFERENCES,
)
@click.option("--timestamp", type=str, help=TIMESTAMP_DESCRIPTION)
@click.option("--security", required=True, type=str, help=SECURITY_STRING_DESCRIPTION)
@click.option("--extract", is_flag=True, show_default=True, default=False, help=EXTRACT_DESCRIPTION)
@click.option("--extract-password", type=str, help=EXTRACT_PASSWORD_DESCRIPTION)
def put(
    y: bool,
    path: str,
    timestamp: str,
    security: str,
    source: str,
    ref: list[str],
    extract: bool,
    extract_password: str,
):
    """Upload all files in PATH to Azul with the specified SOURCE.

    PATH is the location of the file/files/directory.

    SOURCE is the ID of the source to upload the file to.
    SOURCEs can be seen with $ azul sources list.

    A description is required on most uploads, as such, --ref description:text is a soft-requirement.
    You can (& should) have multiple --ref arguments.
    """
    split_refs = [x.split(":", 1) for x in ref]
    refs = {x[0]: x[1].lstrip() for x in split_refs}
    _shared_submit(
        y,
        path,
        security=security,
        timestamp=timestamp,
        source=source,
        source_refs=refs,
        extract=extract,
        extract_password=extract_password,
    )


# azul binaries put-stdin
@binaries.command(cls=ExamplesCommand)
@with_examples(
    "$ cat sample_exe | azul b put-stdin <FILENAME> <SOURCE> --security OFFICIAL",
    "",
    '$ cat sample | azul b put-stdin sample_exe <SOURCE> --security OFFICIAL --ref "description:malware desc"',
    "",
    '$ cat sample | azul b put-stdin sample_exe <SOURCE> --security OFFICIAL --ref "description:malware desc" --timestamp 2012-12-12T11:11:11Z',
    "",
    '$ cat sample | azul b put-stdin sample_exe <SOURCE> --security OFFICIAL --ref "description:malware desc" --timestamp 2012-12-12T11:11:11Z',
)
@click.argument("filename")
@click.argument("source")
@click.option("-y", is_flag=True, show_default=True, default=False, help=NO_CONFIRMATION_PROMPT)
@click.option(
    "--ref",
    type=str,
    multiple=True,
    metavar="KEY:VALUE",
    help=REFERENCES,
)
@click.option("--timestamp", type=str, help=TIMESTAMP_DESCRIPTION)
@click.option("--security", required=True, type=str, help=SECURITY_STRING_DESCRIPTION)
def put_stdin(y: bool, filename: str, source: str, ref: list[str], timestamp: str, security: str):
    """Upload a file from stdin into an Azul SOURCE.

    FILENAME is the name of the file in Azul, and SOURCE is the ID of the source to upload the file to.
    """
    split_refs = [x.split(":", 1) for x in ref]
    refs = {x[0]: x[1].lstrip() for x in split_refs}
    _shared_submit(
        y,
        filename,
        security=security,
        timestamp=timestamp,
        source=source,
        source_refs=refs,
        from_stdin=True,
    )


# azul binaries get-meta
@binaries.command(cls=ExamplesCommand)
@with_examples(
    "$ azul b get-meta <SHA256>",
    "$ azul b get-meta --no-pretty <SHA256>",
    "$ azul b get-meta --output /tmp/metadata.txt <SHA256>",
)
@click.argument("sha256")
@click.option(
    "-o",
    "--output",
    help="Output to a file - use '-' for stdout.",
    default="-",
    show_default=True,
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=True),
)
@click.option(
    "--pretty/--no-pretty",
    help="Render stdout output coloured (default true if terminal, else false).",
    default=os.isatty(sys.stdout.fileno()),
)
def get_meta(sha256: str, output: str, pretty: bool):
    """Get a binary's metadata from Azul by SHA256."""
    try:
        entity = api.binaries_meta.get_meta(sha256)
    except BadResponse as e:
        err = json.loads(e.content).get("detail")[0].get("msg")
        click.echo(f"Error - {err}")
        sys.exit(1)

    if output == "-":
        _print_model(entity, pretty)
    else:
        click.echo(f"saving output to path {output}", err=True)
        with open(output, "w") as f:
            f.write(entity.model_dump_json(indent=4))


# azul binaries get
@binaries.command(cls=ExamplesCommand)
@with_examples(
    "$ azul b get --output /tmp",
    "$ azul b get --term 'size:<1MB \"executable/windows\"'",
    "$ azul b get --output /tmp --term 'size:<1MB \"executable/windows\"'",
    "",
    "$ azul b get --sort-by timestamp",
    "$ azul b get --sort-by timestamp --sort-asc",
    "",
    "$ azul b get -o /tmp --term 'size:<1MB \"executable/windows\"' --sort-by timestamp",
)
@click.option("-o", "--output", help="Download and output to this folder.")
@click.option("--term", help="Search term (refer to UI Explore for suggested search terms)", default="")
@click.option("--max", "--limit", help="Max number of results (Default: 100)", default=100)
@click.option(
    "--sort-by",
    default=None,
    type=click.Choice(
        [
            str(models_restapi.FindBinariesSortEnum.score),
            str(models_restapi.FindBinariesSortEnum.source_timestamp),
            str(models_restapi.FindBinariesSortEnum.timestamp),
        ]
    ),
    help="What property to use when sorting results",
)
@click.option(
    "--sort-asc", default=False, is_flag=True, show_default=True, help="sort by ascending rather than descending."
)
def get(output: str, term: str, max: int, sort_by: models_restapi.FindBinariesSortEnum, sort_asc: bool):
    """Query Azul from CLI, export hashes or download samples.

    Combining multiple filters may lead to unexpected results.
    You can only query multiple attributes over a single authors document.
    """
    if output:
        click.echo(f"saving output to folder {output}")
    else:
        click.echo("no output folder provided, skip download")
    try:
        entity = api.binaries_meta.find(term=term, max_entities=max, sort_prop=sort_by, sort_asc=sort_asc)
    except BadResponse as e:
        try:
            err = json.loads(e.content).get("message")
        except json.JSONDecodeError:
            if isinstance(e.content, bytes):
                err = e.content.decode("utf-8", errors="ignore")
        click.echo(f"Error - {err}")
        sys.exit(1)

    # create output folder
    if output:
        click.echo(f"download to folder {output}")
        if not os.path.exists(output):
            click.echo(f"creating directory: {output}")
            os.mkdir(output)
        if not os.path.isdir(output):
            raise Exception(f"supplied path is not a directory: {output}")

    # print and save found binary
    for hit in entity.items:
        click.echo(hit.sha256)
        if output and hit.sha256:
            content = api.binaries_data.download(hit.sha256)
            if content:
                with open(os.path.join(output, f"{hit.sha256}.cart"), "wb") as f:
                    f.write(content)
            else:
                click.echo("content not found")


# azul binaries download
@binaries.command(cls=ExamplesCommand)
@with_examples(
    "$ azul binaries download <SHA256>",
    "$ azul binaries download --outout /tmp <SHA256>",
    "$ azul b download -o /tmp/ <SHA256>",
)
@click.argument("sha256s", nargs=-1, type=str)
@click.option("-o", "--output", help="Output folder.")
def download(sha256s: tuple[str], output: str):
    """Downloaded the provided SHA256's from Azul in Cart form.

    All provided arguments will be considered SHA256's.
    You can also optionally provide and output folder to not download to the current directory.
    """
    if output:
        click.echo(f"saving output to folder {output}")
    else:
        click.echo("saving file to current directory")
        output = ""

    invalid_sha256s = []
    valid_sha256s = []
    for sha in sha256s:
        if not re.search(api.binaries_data.SHA256_regex, sha):
            invalid_sha256s.append(sha)
        else:
            valid_sha256s.append(sha)

    if len(invalid_sha256s) > 0:
        click.echo(f"\nIgnoring the following provided invalid sha256s:\n  {'\n  '.join(invalid_sha256s)}\n")

    if not valid_sha256s:
        click.echo("No valid sha256's provided stopping now.")
        return

    click.echo(f"Downloading the following sha256's:\n  {'\n  '.join(valid_sha256s)}\n")
    successful_download_paths = []
    for sha in valid_sha256s:
        # Download file and account for fail cases.
        try:
            raw_bytes = api.binaries_data.download(sha)
        except BadResponse404:
            click.echo(f"Error: Could not download the sha256 {sha} because it does not exist in Azul.")
            continue
        except BadResponse as e:
            click.echo(f"Error: Could not download the sha256 {sha} with error {e}")
            continue
        except Exception as e:
            click.echo(f"Error: Could not download the sha256 {sha} with unexpected error {e}")
            continue

        try:
            new_path = os.path.join(output, sha + ".cart")
            with open(new_path, "wb") as f:
                f.write(raw_bytes)

            successful_download_paths.append(new_path)
        except Exception as e:
            click.echo(f"Could not save the sha256 {sha} after successful download with error\n {e}")

    # Nicer new line formatting.
    if len(successful_download_paths) != len(valid_sha256s):
        click.echo("")

    click.echo(f"Download completed for the following files:\n  {'\n  '.join(successful_download_paths)}")


cli.add_command(_client_config)
cli.add_command(binaries)
cli.add_command(binaries, name="b")
cli.add_command(security)
cli.add_command(sources)
cli.add_command(plugins)
if __name__ == "__main__":
    cli()
