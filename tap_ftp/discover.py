import csv
import singer
from singer import metadata

from tap_ftp import client
from tap_ftp.singer_encodings import json_schema

LOGGER = singer.get_logger()


def discover_streams(config):
    streams = []

    conn = client.connection(config)
    
    # Cache directory listings by prefix to avoid redundant LIST operations
    # This significantly speeds up discover when multiple tables use the same prefix
    # Key: (prefix, search_subdirectories), Value: list of all files in that prefix
    dir_listing_cache = {}
    search_subdir = config.get("search_subdirectories", True)

    tables = config['tables']
    for table_spec in tables:
        visible_name = table_spec['table_name'].replace("/", "_")
        original_name = table_spec['table_name']
        if visible_name != original_name:
            LOGGER.info(f"Table name has been renamed from {original_name} to {visible_name} due to unsupported characters.")
        LOGGER.info('Sampling records to determine table JSON schema "%s".', table_spec['table_name'])
        try:
            # Get prefix and check cache
            prefix = table_spec.get('search_prefix', '') or ''
            cache_key = (prefix, search_subdir)
            
            # If we haven't listed this directory yet, do it once and cache
            if cache_key not in dir_listing_cache:
                # Get all files for this prefix (without pattern filtering)
                # This is the expensive operation we want to cache
                all_files = conn.get_files_by_prefix(prefix, search_subdir)
                dir_listing_cache[cache_key] = all_files
            
            # Use cached directory listing and filter by pattern
            cached_files = dir_listing_cache[cache_key]
            matching_files = conn.get_files_matching_pattern(cached_files, table_spec['search_pattern'])
            
            # Get schema using the matching files
            schema = json_schema.get_schema_for_table_with_files(conn, table_spec, matching_files, config)
        except csv.Error as e:
            if "field larger than field limit" in str(e):
                raise Exception(f"CSV file ({original_name}) seems to be corrupted. Please check the file for unclosed quotes. Error: {e}")
            else:
                raise e
        stream_md = metadata.get_standard_metadata(schema,
                                                   key_properties=table_spec.get('key_properties'),
                                                   replication_method='INCREMENTAL')
        streams.append(
            {
                'stream': visible_name,
                'tap_stream_id': original_name,
                'schema': schema,
                'metadata': stream_md
            }
        )

    return streams

