# tap-ftps

[Singer](https://www.singer.io/) tap that extracts data from FTPS (FTP over SSL/TLS) files and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

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

1. Create a `config.json` file with connection details to your FTPS server.

   ```json
   {
        "host": "FTPS_HOST_NAME",
        "port": 21,
        "username": "YOUR_USER",
        "password": "YOUR_PASS",
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
   
   - `host` (required): FTPS server hostname or IP address
   - `port` (optional): FTPS server port (default: 21)
   - `username` (required): FTPS username
   - `password` (required): FTPS password (FTPS uses password authentication, not SSH keys)
   - `tables` (required): Array of table configurations
   - `start_date` (required): Initial date for file-level bookmarking (ISO format: YYYY-MM-DD)
   - `timeout` (optional): Timeout in seconds for data transfers (default: 300 seconds / 5 minutes)
   - `connect_timeout` (optional): Timeout in seconds for initial connection (default: 30 seconds)
   - `decryption_configs` (optional): Configuration for GPG file decryption

   **Table Configuration:**
   
   - `table_name` (required): Name of the stream/table
   - `search_prefix` (required): Directory path on FTPS server to search for files
   - `search_pattern` (required): Regex pattern to match filenames (use `^` and `$` for exact matches)
   - `replication_key_column` (optional): Column name for row-level bookmarking (e.g., "updated_at")
   - `delimiter` (optional): CSV delimiter (default: ",")
   - `encoding` (optional): File encoding (default: auto-detected)

   **Important Notes:**
   
   - **Regex Patterns**: Use `^pattern$` to ensure exact filename matching (e.g., `^products-.*\\.csv$` matches `products-20241217.csv` but not `supplier_products-20241217.csv`)
   - **Row-level Bookmarking**: If `replication_key_column` is specified, the tap will only process rows where the replication key value is greater than the last synced value
   - **File-level Bookmarking**: The tap tracks `last_modified` timestamps and only processes files modified since the last sync
   - **Decryption**: If using the decryption feature, you must pass the configs shown above, including the AWS SSM parameter name for where the decryption private key is stored. In order to retrieve this parameter the runtime environment must have access to SSM through IAM environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN).

## Discovery mode:

The tap can be invoked in discovery mode to find the available tables and
columns in the database:

```bash
$ tap-ftps --config config.json --discover > catalog.json
```

A discovered catalog is output, with a JSON-schema description of each table. A
source table directly corresponds to a Singer stream.

Edit the `catalog.json` and select the streams to replicate by setting `"selected": true` in the stream metadata. Or use this helpful [discovery utility](https://github.com/chrisgoddard/singer-discover).

## Run Tap:

Run the tap like any other singer compatible tap:

```bash
$ tap-ftps --config config.json --catalog catalog.json --state state.json
```

The tap supports:
- **File-level bookmarking**: Only processes files modified since the last sync
- **Row-level bookmarking**: Only processes rows where the replication key value is greater than the last synced value (when `replication_key_column` is configured)
- **Automatic reconnection**: Retries failed connections with exponential backoff
- **Secure data channel**: Uses FTPS with TLS/SSL encryption for both control and data connections

## Features:

- **FTPS Support**: Secure FTP over SSL/TLS with certificate verification disabled (for self-signed certificates)
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
