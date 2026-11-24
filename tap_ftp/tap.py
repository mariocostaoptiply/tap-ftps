import csv
import json
import re
import sys

import singer
from singer import metadata, utils
from terminaltables import AsciiTable
from tap_ftp import client

from tap_ftp.discover import discover_streams
from tap_ftp.stats import STATS
from tap_ftp.sync import sync_stream

REQUIRED_CONFIG_KEYS = ["username", "port", "host", "start_date"]
REQUIRED_DECRYPT_CONFIG_KEYS = ['SSM_key_name', 'gnupghome', 'passphrase']
REQUIRED_TABLE_SPEC_CONFIG_KEYS = ["table_name"]

LOGGER = singer.get_logger()


def _configure_tables(config):
    tables = []
    for key, file_name in config.items():
        if not key.endswith("_file_name") or file_name in (None, ""):
            continue
        stream = key[:-len("_file_name")]
        if not re.fullmatch(r"[A-Za-z0-9_-]+", stream):
            raise ValueError(f"Invalid stream name in {key}")
        table = {
            "table_name": stream,
            "file_name": file_name,
            "key_properties": [],
            "delimiter": "auto",
            "encoding": "utf-8",
        }
        replication_key = config.get(f"{stream}_replication_key_column")
        if replication_key not in (None, ""):
            if not isinstance(replication_key, str) or not replication_key.strip():
                raise ValueError(f"{stream}_replication_key_column must be a column name")
            table["replication_key_column"] = replication_key
        tables.append(table)
    if tables:
        if config.get("tables"):
            raise ValueError("Use either tables or flat stream file fields, not both")
        config["tables"] = tables
    elif not config.get("tables"):
        raise ValueError("Configure at least one <stream>_file_name or a nonempty tables list")


def do_discover(config):
    LOGGER.info("Starting discover")
    streams = discover_streams(config)
    if not streams:
        raise Exception("No streams found")
    catalog = {"streams": streams}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)


def do_sync(config, catalog, state):
    LOGGER.info('Starting sync.')

    for stream in catalog.streams:
        stream_name = stream.stream
        mdata = metadata.to_map(stream.metadata)

        if not stream_is_selected(mdata):
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        singer.write_state(state)
        key_properties = metadata.get(metadata.to_map(stream.metadata), (), "table-key-properties")
        if key_properties is None: 
            key_properties = []
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
        do_discover(args.config)
    elif args.catalog or args.properties:
        do_sync(args.config, args.catalog, args.state)

if __name__=="__main__":
    main()

