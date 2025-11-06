"""High level library flow."""

import json
import os
import sys
from tempfile import SpooledTemporaryFile

import click
import pendulum
from azul_bedrock import models_restapi
from pydantic import BaseModel
from rich.console import Console

from azul_client import config
from azul_client.api import Api
from azul_client.config import _client_config
from azul_client.exceptions import BadResponse, BadResponse404

api: Api = None

SECURITY_STRING_DESCRIPTION = "simple security string (use `azul security` to see available security strings)"
TIMESTAMP_DESCRIPTION = (
    "timestamp for which the file being submitted was sourced in ISO8601 format e.g 2025-05-26T02:11:44Z"
)


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


@click.command(name="security")
@click.option("--full", is_flag=True, show_default=True, default=False, help="show full configuration")
def security(full: bool):
    """List Azul security classification settings."""
    settings = api.security.get_security_settings()
    if not full and settings.get("presets"):
        click.echo("Security Presets:")
        click.echo("\n".join(settings.get("presets")))
    else:
        click.echo(json.dumps(settings, indent=2))


@click.group(name="sources")
def sources():
    """List and get information about specific sources."""
    pass


@sources.command(name="list")
def sources_list():
    """List all of the source ids."""
    all_sources = api.sources.get_all_sources()
    click.echo("Source IDS:")
    click.echo("\n".join(all_sources.keys()))


@sources.command(name="full")
def sources_full():
    """Get the full source information for each source."""
    all_sources = api.sources.get_all_sources()
    all_sources_dumped = {}
    for k, val in all_sources.items():
        all_sources_dumped[k] = val.model_dump()
    click.echo("Sources:")
    click.echo(json.dumps(all_sources_dumped, indent=2))


@sources.command(name="info")
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
                click.echo(f"  required: '{ref.required}'")
            break


@click.group()
def plugins():
    """List and get information for plugins in Azul."""
    pass


@plugins.command(name="list")
def plugins_list():
    """List all of the plugins registered in Azul."""
    plugin_list = api.plugins.get_all_plugins()
    click.echo("Plugins (name version):")
    for p in plugin_list:
        click.echo(f"{p.newest_version.name} {p.newest_version.version}")


@plugins.command(name="info")
@click.argument("name")
@click.option("--version", type=str, help="version of the plugin to get info for (defaults to newest)")
def plugin_info(name: str, version: str):
    """Get the details of a plugin with the provided plugin name."""
    if version:
        try:
            details = api.plugins.get_plugin(name, version)
        except BadResponse404:
            click.echo(f"Plugin {name} {version} does not exist check the version and name.")
            return
        except BadResponse as e:
            click.echo(f"Plugin {name} could not be found due to error {e.message}.")
        click.echo(f"Providing detail for plugin {name} {version}")
        click.echo(details.plugin.model_dump_json(indent=2))

    try:
        plugin_list = api.plugins.get_all_plugins()
    except BadResponse as e:
        click.echo(f"Plugin {name} could not be found due to error {e.message}.")
        return

    for p in plugin_list:
        if p.newest_version.name == name:
            click.echo(f"Providing detail for plugin {p.newest_version.name} {p.newest_version.version}")
            click.echo(p.newest_version.model_dump_json(indent=2))
            return
    click.echo(f"Plugin {name} could not be found, check the name is valid.")


@binaries.command()
@click.argument("sha256")
def check(sha256: str):
    """Check if binary metadata associated with the provided SHA256 is in Azul or not."""
    if api.binaries_meta.check_meta(sha256):
        click.echo("Binary metadata available")
    else:
        click.echo("Binary metadata NOT available")
        sys.exit(1)


@binaries.command()
@click.argument("sha256")
def check_data(sha256: str):
    """Check if a binary in Azul has the original file stored in Azul for the provided SHA256."""
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
    parent_rels: dict = None,
    source: str = "",
    source_refs: dict = None,
):
    """Common class for submitting binaries to Azul."""
    security = security if security else ""
    if not timestamp:
        timestamp = pendulum.now(pendulum.UTC).to_iso8601_string()
    else:
        timestamp = pendulum.parse(timestamp).to_iso8601_string()

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

    click.echo(f"Security: {security}")
    click.echo(f"Timestamp: {timestamp}")
    click.echo(f"Extract: {extract}")
    click.echo(f"Extract Password: {extract_password}")
    if parent:
        click.echo(f"Parent: {parent}")
        click.echo(f"Relationship: {parent_rels}")
    else:
        click.echo(f"Source: {source}")
        click.echo(f"References: {source_refs}")

    if not confirmed and not click.confirm(f"Proceed with upload of {len(input_files)} files?"):
        sys.exit(1)

    # submit each file
    for fullpath, filepath in input_files:
        with open(fullpath, "rb") as f:
            if parent:
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


@binaries.command()
@click.argument("path")
@click.option("-y", is_flag=True, show_default=True, default=False, help="no confirmation prompt")
@click.option("--timestamp", type=str, help=TIMESTAMP_DESCRIPTION)
@click.option("--security", required=True, type=str, help=SECURITY_STRING_DESCRIPTION)
@click.option("--parent", required=True, type=str, help="SHA256 of parent file")
@click.option(
    "-r",
    "--relationship",
    required=True,
    multiple=True,
    type=str,
    help="""relationship information between the uploaded child and the parent in form key:value e.g:
    azul put-child --relationship action:extracted --relationship relationship:friend
    """,
)
@click.option(
    "--extract",
    is_flag=True,
    show_default=True,
    default=False,
    help="extract the provided child file (must be trusted archive)",
)
@click.option("--extract-password", type=str, help="password to use when extracting the child archive")
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
    """Uploads a binary from PATH as a child of a pre-existing parent binary."""
    parsed_relationships = [r.split(":", 1) for r in relationship]
    relation_dict = {item[0]: item[1] for item in parsed_relationships}
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


@binaries.command()
@click.option("-y", is_flag=True, show_default=True, default=False, help="no confirmation prompt")
@click.argument("path")
@click.argument("source")
@click.option(
    "--ref", type=str, multiple=True, help="references for source. e.g. --ref user:llama --ref location:ocean"
)
@click.option("--timestamp", type=str, help=TIMESTAMP_DESCRIPTION)
@click.option("--security", required=True, type=str, help=SECURITY_STRING_DESCRIPTION)
@click.option("--extract", is_flag=True, show_default=True, default=False, help="submitted files are trusted archives")
@click.option("--extract-password", type=str, help="password for trusted archive to be extracted with")
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
    """Upload all files in PATH to Azul SOURCE."""
    split_refs = [x.split(":", 1) for x in ref]
    refs = {x[0]: x[1] for x in split_refs}
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


@binaries.command()
@click.argument("filename")
@click.argument("source")
@click.option("-y", is_flag=True, show_default=True, default=False, help="automatic confirmation")
@click.option(
    "--ref", type=str, multiple=True, help="references for source. e.g. --ref user:llama --ref location:ocean"
)
@click.option("--timestamp", type=str, help=TIMESTAMP_DESCRIPTION)
@click.option("--security", required=True, type=str, help=SECURITY_STRING_DESCRIPTION)
def put_stdin(y: bool, filename: str, source: str, ref: list[str], timestamp: str, security: str):
    """Upload a file from stdin into an Azul source.

    FILENAME is the name of the file in Azul, and SOURCE is the ID of the source to upload the file to.
    """
    split_refs = [x.split(":", 1) for x in ref]
    refs = {x[0]: x[1] for x in split_refs}
    security = security if security else ""

    if not timestamp:
        timestamp = pendulum.now(pendulum.UTC).to_iso8601_string()
    else:
        timestamp = pendulum.parse(timestamp).to_iso8601_string()

    click.echo(f"Filename: {filename}")
    click.echo(f"Source: {source}")
    click.echo(f"References: {refs}")
    click.echo(f"Timestamp: {timestamp}")
    click.echo(f"Security: {security}")

    if not y and not click.confirm("Proceed with upload of file?"):
        sys.exit(1)

    # Read file in chunks into a spooled temporary file.
    chunk_size = 1024 * 1024 * 1024
    with SpooledTemporaryFile(max_size=chunk_size) as spooledFile:
        while chunk := sys.stdin.buffer.read(chunk_size):
            spooledFile.write(chunk)
        spooledFile.seek(0)

        resp = api.binaries_data.upload(
            spooledFile,
            security=security,
            source_id=source,
            filename=filename,
            timestamp=timestamp,
            references=refs,
        )
        click.echo(f"{filename} - {resp.sha256}")


@binaries.command(help="""Get a binary's metadata from Azul by SHA256.""")
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
    """Get metadata for a binary."""
    entity = api.binaries_meta.get_meta(sha256)

    if output == "-":
        _print_model(entity, pretty)
    else:
        click.echo(f"saving output to path {output}", err=True)
        with open(output, "w") as f:
            f.write(entity.model_dump_json(indent=4))


@binaries.command(
    help="""
Find and download samples from Azul.
Combining multiple filters may lead to unexpected results.
You can only query multiple attributes over a single authors document.
"""
)
@click.option("-o", "--output", help="output folder")
@click.option("--term", help="search term (refer to UI Explore for suggested search terms)", default="")
@click.option("--max", help="max number of entities to retrieve", default=100)
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
    """Get all samples matching the criteria and optionally download the files to an output folder."""
    if output:
        click.echo(f"saving output to folder {output}")
    else:
        click.echo("no output folder provided, skip download")

    params = {"term": term, "max_entities": max, "sort_prop": sort_by, "sort_asc": sort_asc}
    kwargs = {x: y for (x, y) in params.items() if y is not None}
    entity = api.binaries_meta.find(**kwargs)

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
        if output:
            content = api.binaries_data.download(hit.sha256)
            if content:
                with open(os.path.join(output, f"{hit.sha256}.cart"), "wb") as f:
                    f.write(content)
            else:
                click.echo("content not found")


cli.add_command(_client_config)
cli.add_command(binaries)
cli.add_command(binaries, name="b")
cli.add_command(security)
cli.add_command(sources)
cli.add_command(plugins)
if __name__ == "__main__":
    cli()
