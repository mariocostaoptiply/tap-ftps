# tap-ftp

FTP and FTPS (FTP over SSL/TLS) extractor. In Hotglue file mode it writes complete CSVs directly into the job's output directory, without a catalog-dependent Singer-to-CSV conversion. Standalone Singer output remains available when no `target_dir` is configured.

## Install:

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac](http://docs.python-guide.org/en/latest/starting/install3/osx/) or
[Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04).

It's recommended to use a virtualenv:

```bash
$ python3 -m venv venv
$ . venv/bin/activate
$ pip install --upgrade pip
$ pip install .
```

## Configuration:

For Hotglue file mode, configure the connection credentials and `files_path` only.
The tap reads every supported CSV in that folder, without searching subfolders.
Output names follow source basenames unless optional config overrides rename them:

```json
{
  "files_path": "/imports",
  "products_file_name": "inventory.csv",
  "products_replication_key_column": "updated_at",
  "suppliers_file_name": "vendors.csv"
}
```

In Hotglue file mode, `products_file_name` renames the exact source filename to
`products.csv`; it does not exclude other files. New output streams require no
connector UI fields or tap changes. See the native file-mode section below.
Extracting a stream does not automatically add downstream ETL support for it.

Standalone Singer mode (without `target_dir`) retains exact-file selection:
every populated `<stream>_file_name` defines a stream dynamically, with no stream
whitelist. At least one filename is required. An absent, null, or empty filename
skips that stream. Stream names in these config keys may contain ASCII letters,
digits, underscores, and hyphens. Use `<stream>_replication_key_column` for its
optional incremental column. Populated filenames determine streams independently
of the supplied catalog; missing files produce no output or bookmark changes.

In standalone Singer mode, sync emits each file's schema from its actual headers
before emitting records, using the same downloaded file. Columns are not restricted by a cached catalog:
new, renamed, and removed columns follow the current header. Values remain nullable
strings, preserving leading zeros. Headers must be unique; `_sdc_source_file`,
`_sdc_source_lineno`, and `_sdc_extra` are reserved metadata columns.
Legacy `tables` connections still honor their catalog's stream and field selection.
Hotglue may still need discovery to refresh its saved Data Streams UI; neither
native folder sync nor standalone flat sync depends on that saved catalog.

Flat files use UTF-8 CSV with no configured key properties. The separator is
detected separately for each file from the first 65,536 decoded characters:
comma, semicolon, tab, or pipe. Discovery and sync use the same reader.
Ambiguous, single-column, or undetectable samples fail clearly instead of guessing.
Legacy tables keep their explicit separator (or the existing comma default);
set a table's `delimiter` to `"auto"` to opt into detection.
Do not combine nonempty flat filename fields with a nonempty `tables` list:
remove the old `tables` configuration when migrating a connection.
Flat overrides build the existing table configurations internally. These fields
supplement connection credentials and `start_date`; they are not a complete config.

### Direct CSV output in Hotglue

Use these source settings alongside the existing connection UI and install URI:

```json
{
  "no_fieldmap": true,
  "connector_props": {
    "static_data": true
  }
}
```

Hotglue injects `target_dir` into the tap configuration. With `files_path` and no
legacy `tables`, the tap lists that folder once and writes every changed supported
file directly as a UTF-8, comma-separated CSV. No filename fields are required.
`Client stock.csv` becomes `Client stock.csv`; `.csv.gz`, `.csv.zip`, and `.zip`
are decompressed to `<basename>.csv`. XLSX and unrelated files are ignored.
Nested folders are not searched.

Optional `<stream>_file_name` fields rename exact filenames to `<stream>.csv`,
without filtering other files. Two sources resolving to the same output name,
including case-only differences, fail before downloads rather than overwriting.
To add an entity later, rename its file or add a config override; no UI field or
tap change is necessary. Names are not guessed into Optiply entities, and the ETL
must still know how to process each output.

Every current source column is preserved, including new columns absent from a
saved catalog. Source separators are auto-detected; values, leading zeros, and
quoted newlines are preserved. The three reserved `_sdc_*` metadata columns are
included, with `_sdc_extra` serialized as JSON. No Singer records or state messages
are written to stdout in this mode.

The output directory must be fresh for each job: existing CSVs cause an error
rather than replaying stale data. Missing or unchanged files produce no CSV.
Changed header-only files produce a CSV header. Empty ZIPs are rejected and
multiple CSV members in one ZIP must have identical headers.
Legacy `tables` configurations retain Singer mode; direct output rejects them.

Pass `--state` to select a state file explicitly. When Hotglue invokes the tap with
only `--config`, the tap instead loads and updates `state.json` beside that config.
An absent state file starts from `start_date`; malformed state fails before any
FTP access. Native checkpoints are keyed by source path, not output name.
Existing stream-keyed bookmarks are migrated when their matching source appears;
unrelated and missing-file state is retained. File modification and row replication
bookmarks survive an output rename, which does not replay an unchanged file.
A stored replication column is retained unless the new stream's config explicitly
overrides it (an empty value disables row filtering). Renaming the source file or
changing its folder creates a new source identity.

Only files with a newer server modification timestamp are downloaded. With a row
replication key, only newer rows are written. Changing content without advancing
the server modification timestamp is not detected.

CSV files are published atomically after each successful read, and state is
replaced atomically only after every discovered source succeeds. A failed sync
does not advance the saved checkpoint. Hotglue must preserve its own job-level
state rollback when ETL or export fails; verify that handoff in dev before cutover.
Direct mode rejects `delete_after_sync`: source files must remain available for
replay after a later stream, ETL, or export failure.

For a local run, add `"target_dir": "./sync-output"` to the folder config, then run:

```bash
tap-ftp --config config.json
```

Use a new output directory for the next run while retaining the same state file.
Do not enable file-mode source settings against an older Singer-only install.
After updating the install URI, validate one dev job for CSV headers and values,
then an unchanged job for file skipping, and a changed-file job for row filtering.



Use `files_path` for one shared FTP directory and `file_name` for each exact file.
Together with the connection fields and `start_date` shown below, the file-selection config is:

```json
{
  "files_path": "/imports",
  "tables": [
    {
      "table_name": "products",
      "file_name": "inventory.csv",
      "replication_key_column": "updated_at"
    }
  ]
}
```

`table_name` is the Singer stream name consumed by the ETL; it does not rename CSV
columns. Exact filenames are case-sensitive and literal, including characters such
as `[` and `.`. They must not contain directory separators or control characters.
Exact-file mode requires a nonempty `files_path` (`"."` means the FTP login directory)
and never searches subdirectories. It takes precedence over any legacy
`search_prefix` or `search_pattern` on that table.

Existing regex configurations remain supported unchanged, as in this legacy example:

1. Create a `config.json` file with connection details to your FTP/FTPS server.

   ```json
   {
        "host": "FTP_HOST_NAME",
        "port": 21,
        "username": "YOUR_USER",
        "password": "YOUR_PASS",
        "use_ssl": true,
        "tables": [
            {
                "table_name": "products",
                "search_prefix": "/data",
                "search_pattern": "^products-.*\\.csv$",
                "replication_key_column": "updated_at",
                "delimiter": ","
            }
        ],
        "start_date": "2024-01-01",
        "timeout": 300,
        "connect_timeout": 30,
        "decryption_configs": {
            "SSM_key_name": "SSM_PARAMETER_KEY_NAME",
            "gnupghome": "/your/dir/.gnupg",
            "passphrase": "your_gpg_passphrase"
        }
   }
   ```

   **Configuration Options:**
   
   - `host` (required): FTP/FTPS server hostname or IP address
   - `port` (optional): FTP/FTPS server port (default: 21)
   - `username` (required): FTP/FTPS username
   - `password` (required): FTP/FTPS password (FTP/FTPS uses password authentication, not SSH keys)
   - `use_ssl` (optional): Enable FTPS (FTP over SSL/TLS). Default: false (uses plain FTP)
   - `tables` (required without flat filenames): Array of table configurations
   - `files_path` (required with `file_name`): Shared FTP/FTPS directory; also supplies the default legacy `search_prefix`
   - `start_date` (required): Initial date for file-level bookmarking (ISO format: YYYY-MM-DD)
   - `timeout` (optional): Timeout in seconds for data transfers (default: 300 seconds / 5 minutes)
   - `connect_timeout` (optional): Timeout in seconds for initial connection (default: 30 seconds)
   - `decryption_configs` (optional): Configuration for GPG file decryption

   **Table Configuration:**
   
   - `table_name` (required): Name of the stream/table
   - `file_name` (optional): Exact filename inside `files_path`; takes precedence over legacy search settings
   - `search_prefix` (optional): Legacy directory override; defaults to `files_path`, then the FTP login directory
   - `search_pattern` (required without `file_name`): Legacy regex matching filenames; recursive search remains enabled by default
   - `replication_key_column` (optional): Column name for row-level bookmarking (e.g., "updated_at")
   - `delimiter` (optional): CSV delimiter (default: ",")
   - `encoding` (optional): File encoding (default: auto-detected)

   **Important Notes:**
   
   - **Regex Patterns**: Use `^pattern$` to ensure exact filename matching (e.g., `^products-.*\\.csv$` matches `products-20241217.csv` but not `supplier_products-20241217.csv`)
   - **File Selection**: Each table needs `file_name` or a nonempty `search_pattern`; omission never means match all files. Missing or unchanged files emit no records and do not advance that stream's bookmarks.
   - **Singer State Compatibility**: Without native file mode, bookmarks remain keyed by `table_name`, not the filename. Keep the stream name when migrating Singer connections. Changing a file or folder does not reset those Singer bookmarks. Native folder mode instead tracks each source path as described above.
   - **Row-level Bookmarking**: If `replication_key_column` is specified, the tap will only process rows where the replication key value is greater than the last synced value
   - **File-level Bookmarking**: The tap tracks `last_modified` timestamps and only processes files modified since the last sync
   - **Decryption**: If using the decryption feature, you must pass the configs shown above, including the AWS SSM parameter name for where the decryption private key is stored. In order to retrieve this parameter the runtime environment must have access to SSM through IAM environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN).

## Discovery mode:

The tap can be invoked in discovery mode to find the available tables and
columns in the database:

```bash
$ tap-ftp --config config.json --discover > catalog.json
```

A discovered catalog is output, with a JSON-schema description of each table. A
source table directly corresponds to a Singer stream.

Edit the `catalog.json` and select the streams to replicate by setting `"selected": true` in the stream metadata. Or use this helpful [discovery utility](https://github.com/chrisgoddard/singer-discover).

## Run Tap:

Run the tap like any other singer compatible tap:

```bash
$ tap-ftp --config config.json --catalog catalog.json --state state.json
```

The tap supports:
- **File-level bookmarking**: Only processes files modified since the last sync
- **Row-level bookmarking**: Only processes rows where the replication key value is greater than the last synced value (when `replication_key_column` is configured)
- **Automatic reconnection**: Retries failed connections with exponential backoff
- **FTP and FTPS Support**: Supports both plain FTP and FTPS (FTP over SSL/TLS) based on `use_ssl` configuration

## Features:

- **FTP/FTPS Support**: Supports both plain FTP and secure FTPS (FTP over SSL/TLS) with certificate verification disabled for self-signed certificates (when `use_ssl=true`)
- **Incremental Sync**: File-level and row-level bookmarking for efficient incremental updates
- **CSV Processing**: Automatic CSV parsing with configurable delimiters
- **Schema Discovery**: Automatic schema inference from CSV files
- **GPG Decryption**: Optional GPG file decryption with AWS SSM key retrieval
- **Error Handling**: Robust error handling with automatic retries and connection recovery

## To run tests:

1. Install python dependencies in a virtual env and run unit and integration tests
```bash
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install .
  pip install tox
```

2. To run unit tests:
```bash
  tox
```

## Local FTPS Server Testing:

For local testing, you can use the Docker-based FTPS server in `spike/ftps/`:

```bash
# Build and run FTPS server
docker build -t ftps-server spike/ftps/
docker run -d -p 21:21 -p 30000-30059:30000-30059 --name ftps-server ftps-server

# Test connection
python3 spike/ftps/test_connection.py
```

## License

Apache License Version 2.0

See [LICENSE](LICENSE) to see the full text.
