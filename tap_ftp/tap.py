import csv
import json
import re
import sys
from pathlib import Path

import singer
from singer import metadata, utils
from terminaltables import AsciiTable
from tap_ftp import client

from tap_ftp.discover import discover_streams
from tap_ftp.file_output import atomic_output
from tap_ftp.stats import STATS
from tap_ftp.sync import sync_stream

REQUIRED_CONFIG_KEYS = ["username", "port", "host", "start_date"]
REQUIRED_DECRYPT_CONFIG_KEYS = ['SSM_key_name', 'gnupghome', 'passphrase']
REQUIRED_TABLE_SPEC_CONFIG_KEYS = ["table_name"]

LOGGER = singer.get_logger()


def _file_table(stream, file_name, config):
    table = {
        "table_name": stream, "file_name": file_name, "key_properties": [],
        "delimiter": "auto", "encoding": "utf-8", "_dynamic_schema": True,
    }
    replication_key = config.get(f"{stream}_replication_key_column")
    if replication_key not in (None, ""):
        if not isinstance(replication_key, str) or not replication_key.strip():
            raise ValueError(f"{stream}_replication_key_column must be a column name")
        table["replication_key_column"] = replication_key
    return table


def _configure_tables(config):
    tables = []
    folder_mode = "target_dir" in config and not config.get("tables")
    for key, file_name in config.items():
        if not key.endswith("_file_name") or file_name in (None, ""):
            continue
        stream = key[:-len("_file_name")]
        if not re.fullmatch(r"[A-Za-z0-9_-]+", stream):
            raise ValueError(f"Invalid stream name in {key}")
        tables.append(_file_table(stream, file_name, config))
    if tables:
        if config.get("tables"):
            raise ValueError("Use either tables or flat stream file fields, not both")
        config["tables"] = tables
    elif not config.get("tables") and not folder_mode:
        raise ValueError("Configure at least one <stream>_file_name or a nonempty tables list")
    if folder_mode:
        folder = config.get("files_path")
        if not isinstance(folder, str) or not folder.strip():
            raise ValueError("files_path must be a nonempty FTP folder")
        config["tables"] = tables
        config["_folder_mode"] = True


def _configure_folder_files(config, state):
    overrides = {}
    for table in config["tables"]:
        name = table["file_name"]
        if name in overrides:
            raise ValueError(f"More than one rename is configured for file: {name}")
        overrides[name] = table
    conn = client.connection(config)
    try:
        files = conn.get_files_by_prefix(config["files_path"], search_subdirectories=False)
    finally:
        conn.close()
    tables = []
    output_names = set()
    for file in sorted(files, key=lambda item: item["filepath"]):
        name = Path(file["filepath"]).name
        lower = name.lower()
        if lower.endswith(".csv.gz"):
            stream = name[:-7]
        elif lower.endswith(".zip"):
            stream = name[:-4]
            if stream.lower().endswith(".csv"):
                stream = stream[:-4]
        elif lower.endswith(".csv"):
            stream = name[:-4]
        else:
            continue
        table = overrides.get(name) or _file_table(stream, name, config)
        table = client.resolve_table_spec(table, config)
        output_name = table["table_name"]
        if not output_name.strip() or output_name in (".", ".."):
            raise ValueError(f"File has no usable output name: {name}")
        if output_name.casefold() in output_names:
            raise ValueError(f"Files would produce the same output CSV: {output_name}")
        output_names.add(output_name.casefold())
        table["_file_spec"] = file
        table["_bookmark_key"] = file["filepath"]
        tables.append(table)
    bookmarks = state.get("bookmarks", {})
    for table in tables:
        source_key = table["_bookmark_key"]
        output_name = table["table_name"]
        old = bookmarks.get(output_name)
        # Legacy flat names exclude the dots and separators present in source paths.
        if (
            source_key not in bookmarks
            and re.fullmatch(r"[A-Za-z0-9_-]+", output_name)
            and isinstance(old, dict) and "modified_since" in old
        ):
            bookmarks[source_key] = bookmarks.pop(output_name)
        saved = bookmarks.get(source_key, {})
        if f"{output_name}_replication_key_column" not in config and saved.get("replication_key_column"):
            table["replication_key_column"] = saved["replication_key_column"]
    config["tables"] = tables


def do_discover(config):
    LOGGER.info("Starting discover")
    streams = discover_streams(config)
    if not streams and not config.get("_folder_mode"):
        raise Exception("No streams found")
    catalog = {"streams": streams}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)


def do_sync(config, catalog, state):
    LOGGER.info('Starting sync.')
    dynamic_tables = [table for table in config["tables"] if table.get("_dynamic_schema")]
    if dynamic_tables or config.get("_folder_mode"):
        catalog = singer.Catalog.from_dict({"streams": [{
            "stream": table["table_name"],
            "tap_stream_id": table["table_name"],
            "schema": {},
            "metadata": [{"breadcrumb": [], "metadata": {"selected": True}}],
        } for table in dynamic_tables]})

    for stream in catalog.streams:
        stream_name = stream.stream
        mdata = metadata.to_map(stream.metadata)

        if not stream_is_selected(mdata):
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        if not config.get("target_dir"):
            singer.write_state(state)
        key_properties = metadata.get(metadata.to_map(stream.metadata), (), "table-key-properties")
        if key_properties is None: 
            key_properties = []
        if not dynamic_tables:
            singer.write_schema(stream_name, stream.schema.to_dict(), key_properties)

        LOGGER.info("%s: Starting sync", stream_name)
        try:
            counter_value = sync_stream(config, state, stream)
            LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)
        except csv.Error as e:
            if "field larger than field limit" in str(e):
                raise Exception(f"CSV file ({stream_name}) seems to be corrupted. Please check the file for unclosed quotes. Error:{e}")
            else:
                raise e

    headers = [['table_name',
                'search prefix',
                'search pattern',
                'file path',
                'row count',
                'last_modified']]

    rows = []

    for table_name, table_data in STATS.items():
        for filepath, file_data in table_data['files'].items():
            rows.append([table_name,
                         table_data['search_prefix'],
                         table_data['search_pattern'],
                         filepath,
                         file_data['row_count'],
                         file_data['last_modified']])

    data = headers + rows
    table = AsciiTable(data, title='Extraction Summary')
    LOGGER.info("\n\n%s", table.table)
    LOGGER.info('Done syncing.')


@singer.utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    _configure_tables(args.config)
    # validate tables config
    for table in args.config.get('tables'):
        utils.check_config(table, REQUIRED_TABLE_SPEC_CONFIG_KEYS)
        client.resolve_table_spec(table, args.config)

    decrypt_configs = args.config.get('decryption_configs')
    if decrypt_configs:
        # validate decryption configs
        utils.check_config(decrypt_configs, REQUIRED_DECRYPT_CONFIG_KEYS)
    
    # FTP/FTPS requires password (not SSH keys like SFTP)
    password = args.config.get("password")
    if not password:
        raise Exception("Config is missing a password. FTP/FTPS requires password authentication.")
    
    if args.discover:
        if args.config.get("_folder_mode"):
            _configure_folder_files(args.config, {})
        do_discover(args.config)
    elif (args.catalog or args.properties or
          args.config.get("_folder_mode") or
          any(table.get("_dynamic_schema") for table in args.config["tables"])):
        state_path = None
        if "target_dir" in args.config:
            target_dir = args.config["target_dir"]
            if not isinstance(target_dir, str) or not target_dir.strip():
                raise ValueError("target_dir must be a nonempty job output directory")
            if not all(table.get("_dynamic_schema") for table in args.config["tables"]):
                raise ValueError("Direct file output requires flat <stream>_file_name configuration")
            if args.config.get("delete_after_sync"):
                raise ValueError("Direct file output cannot delete source files needed for failed-job replay")
            output_dir = Path(target_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            # Job output must not replay CSVs left by an earlier local run.
            if any(output_dir.glob("*.csv")):
                raise ValueError("target_dir already contains CSVs; use a fresh job output directory")
            state_path = Path(getattr(args, "state_path", Path(args.config_path).with_name("state.json")))
            if not hasattr(args, "state_path") and state_path.exists():
                args.state = utils.load_json(state_path)
            if not isinstance(args.state, dict) or not isinstance(args.state.get("bookmarks", {}), dict):
                raise ValueError("State must be a JSON object with an optional bookmarks object")
        if args.config.get("_folder_mode"):
            _configure_folder_files(args.config, args.state)
        do_sync(args.config, args.catalog, args.state)
        if state_path is not None:
            with atomic_output(state_path) as handle:
                json.dump(args.state, handle)

if __name__=="__main__":
    main()

