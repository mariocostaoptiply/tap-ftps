import csv
import gzip
import io
import json
import socket
from unittest.mock import Mock

import pytest
import singer

from tap_ftp import client, discover, stats, tap
from tap_ftp.singer_encodings import csv_handler, json_schema


@pytest.fixture(autouse=True)
def isolated_job(monkeypatch):
    def forbid_network(*args, **kwargs):
        raise AssertionError("File selection tests must not access the network")

    monkeypatch.setattr(socket.socket, "connect", forbid_network)
    monkeypatch.setattr(socket.socket, "connect_ex", forbid_network)
    client._mlsd_support_cache.clear()
    stats.STATS.clear()
    yield
    client._mlsd_support_cache.clear()
    stats.STATS.clear()


@pytest.fixture
def ftp(monkeypatch):
    source = {
        "files": {
            "/imports/inventory[1].csv": (
                b"id,name,updated_at\np-1,Widget,2026-11-20T12:00:00Z\n"
                b"p-2,Gadget,2026-11-20T13:00:00Z\n"
            ),
            "/imports/inventory1.csv": b"wrong_column\nwrong\n",
            "/imports/inventory[1].csv.bak": b"wrong_column\nwrong\n",
            "/imports/archive/inventory[1].csv": b"wrong_column\nwrong\n",
        },
        "modified": "20261121120000",
        "listings": [],
        "downloads": [],
    }

    def listing(prefix):
        source["listings"].append(prefix)
        entries = {}
        for path, contents in source["files"].items():
            if not path.startswith(prefix.rstrip("/") + "/"):
                continue
            relative = path[len(prefix.rstrip("/")) + 1:]
            name, separator, _ = relative.partition("/")
            entries[name] = {
                "type": "dir" if separator else "file",
                "size": str(len(contents)),
                "modify": source["modified"],
            }
        return list(entries.items())

    def connection(config):
        conn = client.FTPConnection("synthetic.invalid", "synthetic")
        conn.ftp = Mock(mlsd=Mock(side_effect=listing))
        return conn

    def file_handle(conn, file_spec, decryption_configs=None):
        path = file_spec["filepath"]
        source["downloads"].append(path)
        return io.BytesIO(source["files"][path])

    monkeypatch.setattr(client, "connection", connection)
    monkeypatch.setattr(client.FTPConnection, "get_file_handle", file_handle)
    return source


def exact_config(**table_options):
    return {
        "files_path": "/imports",
        "start_date": "2000-01-01T00:00:00Z",
        "tables": [{
            "table_name": "products",
            "file_name": "inventory[1].csv",
            "replication_key_column": "updated_at",
            **table_options,
        }],
    }


def selected_catalog(config):
    streams = discover.discover_streams(config)
    for stream in streams:
        for item in stream["metadata"]:
            if not item["breadcrumb"]:
                item["metadata"]["selected"] = True
    return singer.Catalog.from_dict({"streams": streams})


def messages(capsys):
    return [json.loads(line) for line in capsys.readouterr().out.splitlines()]


@pytest.mark.parametrize("delimiter", [",", ";", "\t", "|"])
@pytest.mark.parametrize("compressed", [False, True])
def test_auto_separator_preserves_quoted_values_and_all_rows(delimiter, compressed):
    rows = [
        ["p-1", 'Widget, semi; tab\tpipe| "quoted"\nsecond line'],
        ["p-2", "Gadget"],
    ]
    text = io.StringIO()
    writer = csv.writer(text, delimiter=delimiter)
    writer.writerow(["id", "name"])
    writer.writerows(rows)
    data = text.getvalue().encode("utf-8")
    if compressed:
        data = gzip.compress(data)
    readers = csv_handler.get_row_iterators(
        io.BytesIO(data),
        options={"delimiter": "auto", "encoding": "utf-8",
                 "file_name": "products.csv.gz" if compressed else "products.csv"},
        infer_compression=True,
    )
    records = [row for reader in readers for row in reader]
    assert records == [{"id": key, "name": name} for key, name in rows]


@pytest.mark.parametrize("data", [
    b"id,name;code\np-1,Widget;A\n",
    b"id\np-1\n",
    b"id:name\np-1:Widget\n",
    b"",
])
def test_auto_separator_rejects_ambiguous_or_undetectable_files(data):
    with pytest.raises(ValueError, match="unique CSV separator"):
        csv_handler.get_row_iterator(io.BytesIO(data), {"delimiter": "auto", "encoding": "utf-8"})


@pytest.mark.parametrize("options", [{}, {"delimiter": ","}])
def test_legacy_separator_is_not_reinterpreted(options):
    reader = csv_handler.get_row_iterator(
        io.BytesIO(b"id,name;code\np-1,Widget;A\n"), {"encoding": "utf-8", **options},
    )
    assert list(reader) == [{"id": "p-1", "name;code": "Widget;A"}]


@pytest.mark.parametrize("delimiter", [",", ";", "\t", "|"])
def test_flat_auto_separator_discovery_sync_and_replication(ftp, capsys, delimiter):
    name = "Widget, semi; tab\tpipe| with\nnotes"
    for filename, separator, header, rows in (
        ("inventory[1].csv", delimiter, ["id", "name", "updated_at"], [
            ["p-1", name, "2026-11-20T12:00:00Z"],
            ["p-2", "Gadget", "2026-11-20T13:00:00Z"],
        ]),
        ("vendors.csv", ";", ["id", "name", "Datetime"], [
            ["s-1", "Vendor, Inc", "2026-11-20T12:00:00Z"],
        ]),
    ):
        text = io.StringIO()
        writer = csv.writer(text, delimiter=separator)
        writer.writerow(header)
        writer.writerows(rows)
        ftp["files"][f"/imports/{filename}"] = text.getvalue().encode("utf-8")
    config = {
        "files_path": "/imports", "start_date": "2000-01-01T00:00:00Z",
        "products_file_name": "inventory[1].csv", "products_replication_key_column": "updated_at",
        "suppliers_file_name": "vendors.csv", "suppliers_replication_key_column": "Datetime",
    }
    tap._configure_tables(config)
    catalog = selected_catalog(config)
    assert {"id", "name", "updated_at"} <= set(catalog.streams[0].schema.to_dict()["properties"])
    assert {"id", "name", "Datetime"} <= set(catalog.streams[1].schema.to_dict()["properties"])
    tap.do_sync(config, catalog, {})
    first = messages(capsys)
    records = [message for message in first if message["type"] == "RECORD"]
    assert {(record["stream"], record["record"]["id"]): record["record"]["name"] for record in records} == {
        ("products", "p-1"): name, ("products", "p-2"): "Gadget", ("suppliers", "s-1"): "Vendor, Inc",
    }
    state = first[-1]["value"]
    assert state["bookmarks"]["suppliers"]["replication_key_column"] == "Datetime"
    assert state["bookmarks"]["suppliers"]["replication_key_value"] == "2026-11-20T12:00:00+00:00"
    checkpoint = json.loads(json.dumps(state))
    tap.do_sync(config, catalog, state)
    assert not any(message["type"] == "RECORD" for message in messages(capsys))
    assert state == checkpoint
    ftp["modified"] = "20261122120000"
    ftp["files"]["/imports/vendors.csv"] += b"s-2;New Vendor;2026-11-22T12:00:00Z\n"
    tap.do_sync(config, catalog, state)
    changed = messages(capsys)
    records = [message for message in changed if message["type"] == "RECORD"]
    assert [(record["stream"], record["record"]["id"]) for record in records] == [("suppliers", "s-2")]
    assert changed[-1]["value"]["bookmarks"]["suppliers"]["replication_key_value"] == (
        "2026-11-22T12:00:00+00:00"
    )


def test_auto_separator_failure_preserves_bookmarks(ftp, capsys):
    config = {
        "files_path": "/imports", "start_date": "2000-01-01T00:00:00Z",
        "products_file_name": "inventory[1].csv",
    }
    tap._configure_tables(config)
    catalog = selected_catalog(config)
    ftp["files"]["/imports/inventory[1].csv"] = b"id,name;code\np-1,Widget;A\n"
    state = {"bookmarks": {"products": {"modified_since": "2026-11-19T00:00:00+00:00"}}}
    previous = json.loads(json.dumps(state))
    with pytest.raises(ValueError, match="unique CSV separator"):
        tap.do_sync(config, catalog, state)
    assert state == previous
    assert not any(message["type"] == "RECORD" for message in messages(capsys))


def test_exact_file_discovery_uses_literal_name_and_shared_folder(ftp):
    config = exact_config(search_prefix="/old", search_pattern=".*")
    catalog = selected_catalog(config)

    stream = catalog.streams[0]
    assert stream.stream == stream.tap_stream_id == "products"
    assert {"id", "name", "updated_at"} <= set(stream.schema.to_dict()["properties"])
    assert "wrong_column" not in stream.schema.to_dict()["properties"]
    assert ftp["downloads"] == ["/imports/inventory[1].csv"]
    assert ftp["listings"] == ["/imports"]
    assert config["tables"][0]["search_pattern"] == ".*"


def test_discovery_reuses_shared_folder_for_distinct_streams(ftp):
    ftp["files"]["/imports/vendors.csv"] = b"supplier_id,name\ns-1,Vendor\n"
    config = exact_config()
    config["tables"].append({"table_name": "suppliers", "file_name": "vendors.csv"})

    catalog = selected_catalog(config)
    assert [stream.stream for stream in catalog.streams] == ["products", "suppliers"]
    assert ftp["listings"] == ["/imports"]
    assert set(ftp["downloads"]) == {"/imports/inventory[1].csv", "/imports/vendors.csv"}


@pytest.mark.parametrize("streams", [
    pytest.param((
        "products", "product_compositions", "suppliers", "supplier_products",
        "sell_orders", "sell_order_lines", "buy_orders", "buy_order_lines", "receipt_lines",
    ), id="existing"),
    pytest.param(("stocks", "warehouse_levels", "custom-prices"), id="dynamic"),
])
def test_flat_cli_discovers_and_syncs_configured_streams_and_resumes(ftp, tmp_path, monkeypatch, capsys, streams):
    config = {
        "host": "synthetic.invalid", "port": 21, "username": "synthetic", "password": "synthetic",
        "files_path": "/imports", "start_date": "2000-01-01T00:00:00Z",
    }
    config["unused_stream_file_name"] = ""
    config["stocks_file_name_hint"] = "not-a-stream.csv"
    config["file_name"] = "not-a-stream.csv"
    ftp["files"].clear()
    for stream in streams:
        filename = f"{stream}[1].csv"
        config[f"{stream}_file_name"] = filename
        config[f"{stream}_replication_key_column"] = "updated_at"
        ftp["files"][f"/imports/{filename}"] = (
            f"id,name,updated_at\n{stream}-1,Example,2026-11-20T12:00:00Z\n".encode()
        )
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))
    monkeypatch.setattr("sys.argv", ["tap-ftp", "--config", str(config_path), "--discover"])
    tap.main()
    catalog = json.loads(capsys.readouterr().out)
    assert [stream["stream"] for stream in catalog["streams"]] == list(streams)
    assert ftp["listings"] == ["/imports"]
    for stream in catalog["streams"]:
        assert "id" in stream["schema"]["properties"]
        for item in stream["metadata"]:
            if not item["breadcrumb"]:
                item["metadata"]["selected"] = True
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(json.dumps(catalog))
    sync_args = ["tap-ftp", "--config", str(config_path), "--catalog", str(catalog_path)]
    monkeypatch.setattr("sys.argv", sync_args)
    tap.main()
    output = messages(capsys)
    records = [message for message in output if message["type"] == "RECORD"]
    assert {(record["stream"], record["record"]["id"]) for record in records} == {
        (stream, f"{stream}-1") for stream in streams
    }
    assert {record["record"]["_sdc_source_file"] for record in records} == {
        f"/imports/{stream}[1].csv" for stream in streams
    }
    state = output[-1]["value"]
    for stream in streams:
        checkpoint = state["bookmarks"][stream]
        assert checkpoint["modified_since"] == "2026-11-21T12:00:00+00:00"
        assert checkpoint["replication_key_column"] == "updated_at"
        assert checkpoint["replication_key_value"] == "2026-11-20T12:00:00+00:00"
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps(state))
    ftp["downloads"].clear()
    monkeypatch.setattr("sys.argv", sync_args + ["--state", str(state_path)])
    tap.main()
    resumed = messages(capsys)
    assert not any(message["type"] == "RECORD" for message in resumed)
    assert next(message["value"] for message in reversed(resumed) if message["type"] == "STATE") == state
    assert ftp["downloads"] == []


@pytest.mark.parametrize("blank", [None, ""])
def test_flat_cli_skips_blank_filenames_and_allows_no_replication_key(ftp, tmp_path, monkeypatch, capsys, blank):
    config = {
        "host": "synthetic.invalid", "port": 21, "username": "synthetic", "password": "synthetic",
        "files_path": "/imports", "start_date": "2000-01-01T00:00:00Z",
        "products_file_name": "inventory[1].csv",
        "products_replication_key_column": blank,
        "suppliers_file_name": blank,
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))
    monkeypatch.setattr("sys.argv", ["tap-ftp", "--config", str(config_path), "--discover"])
    tap.main()
    catalog = json.loads(capsys.readouterr().out)
    assert [stream["stream"] for stream in catalog["streams"]] == ["products"]
    for item in catalog["streams"][0]["metadata"]:
        if not item["breadcrumb"]:
            item["metadata"]["selected"] = True
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(json.dumps(catalog))
    monkeypatch.setattr("sys.argv", [
        "tap-ftp", "--config", str(config_path), "--catalog", str(catalog_path),
    ])
    tap.main()
    output = messages(capsys)
    records = [message for message in output if message["type"] == "RECORD"]
    assert [record["record"]["id"] for record in records] == ["p-1", "p-2"]
    checkpoint = output[-1]["value"]["bookmarks"]["products"]
    assert checkpoint["modified_since"] == "2026-11-21T12:00:00+00:00"
    assert "replication_key_column" not in checkpoint


def test_flat_config_rejects_conflicting_legacy_tables():
    config = exact_config()
    config["products_file_name"] = "different.csv"
    with pytest.raises(ValueError, match="not both"):
        tap._configure_tables(config)
    assert config["tables"][0]["file_name"] == "inventory[1].csv"


@pytest.mark.parametrize("tables", [None, []])
def test_flat_config_rejects_no_selected_files(tables):
    with pytest.raises(ValueError, match="at least one"):
        tap._configure_tables({"tables": tables, "products_file_name": ""})


@pytest.mark.parametrize("replication_key", [42, [], " "])
def test_flat_config_rejects_invalid_replication_column(replication_key):
    config = {
        "products_file_name": "inventory.csv",
        "products_replication_key_column": replication_key,
    }
    with pytest.raises(ValueError, match="products_replication_key_column"):
        tap._configure_tables(config)


@pytest.mark.parametrize("prefix", ["", "stock/levels", "stock\\levels", "../stocks", "stock levels", "stocks\n"])
def test_flat_config_rejects_ambiguous_or_unsafe_stream_names(prefix):
    with pytest.raises(ValueError, match="stream name"):
        tap._configure_tables({f"{prefix}_file_name": "inventory.csv"})


def test_cli_discovery_accepts_filename_without_legacy_fields(ftp, tmp_path, monkeypatch, capsys):
    config = exact_config()
    config.update({"host": "synthetic.invalid", "port": 21, "username": "synthetic", "password": "synthetic"})
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))
    monkeypatch.setattr("sys.argv", ["tap-ftp", "--config", str(config_path), "--discover"])

    tap.main()
    catalog = json.loads(capsys.readouterr().out)
    assert catalog["streams"][0]["stream"] == "products"
    assert "id" in catalog["streams"][0]["schema"]["properties"]
    assert ftp["downloads"] == ["/imports/inventory[1].csv"]


def test_exact_sync_emits_named_stream_and_resumes_incrementally(ftp, capsys):
    config = exact_config()
    catalog = selected_catalog(config)
    state = {}
    ftp["downloads"].clear()

    tap.do_sync(config, catalog, state)
    first = messages(capsys)
    records = [message for message in first if message["type"] == "RECORD"]
    assert [(record["stream"], record["record"]["id"]) for record in records] == [
        ("products", "p-1"), ("products", "p-2"),
    ]
    assert {record["record"]["_sdc_source_file"] for record in records} == {
        "/imports/inventory[1].csv",
    }
    checkpoint = first[-1]["value"]["bookmarks"]["products"]
    assert checkpoint["modified_since"] == "2026-11-21T12:00:00+00:00"
    assert checkpoint["replication_key_column"] == "updated_at"
    assert checkpoint["replication_key_value"] == "2026-11-20T13:00:00+00:00"
    assert stats.STATS["products"]["files"]["/imports/inventory[1].csv"]["row_count"] == 2

    resumed = json.loads(json.dumps(first[-1]["value"]))
    ftp["downloads"].clear()
    tap.do_sync(config, catalog, resumed)
    assert not any(message["type"] == "RECORD" for message in messages(capsys))
    assert ftp["downloads"] == []
    assert resumed["bookmarks"]["products"] == checkpoint

    ftp["modified"] = "20261122120000"
    ftp["files"]["/imports/inventory[1].csv"] += b"p-3,New,2026-11-22T12:00:00Z\n"
    tap.do_sync(config, catalog, resumed)
    updated = messages(capsys)
    records = [message for message in updated if message["type"] == "RECORD"]
    assert [record["record"]["id"] for record in records] == ["p-3"]
    assert updated[-1]["value"]["bookmarks"]["products"]["replication_key_value"] == (
        "2026-11-22T12:00:00+00:00"
    )


def test_standalone_schema_discovery_accepts_exact_file_config(ftp):
    config = exact_config()
    schema = json_schema.get_schema_for_table(client.connection(config), config["tables"][0], config)

    assert "id" in schema["properties"]
    assert "wrong_column" not in schema["properties"]
    assert ftp["downloads"] == ["/imports/inventory[1].csv"]
    assert ftp["listings"] == ["/imports"]


@pytest.mark.parametrize("shared_folder", [False, True])
def test_legacy_regex_discovery_and_sync_still_match_multiple_files(ftp, capsys, shared_folder):
    ftp["files"] = {
        "/imports/products-a.csv": b"id,name\np-1,First\n",
        "/imports/products-b.csv": b"id,name\np-2,Second\n",
        "/imports/archive/products-c.csv": b"id,name\np-3,Third\n",
        "/imports/suppliers.csv": b"id,name\ns-1,Supplier\n",
    }
    config = {
        "start_date": "2000-01-01T00:00:00Z",
        "tables": [{"table_name": "products", "search_pattern": r"^products-.*\.csv$"}],
    }
    if shared_folder:
        config["files_path"] = "/imports"
    else:
        config["tables"][0]["search_prefix"] = "/imports"
    catalog = selected_catalog(config)
    ftp["downloads"].clear()

    tap.do_sync(config, catalog, {})
    output = messages(capsys)
    records = [message for message in output if message["type"] == "RECORD"]
    assert {record["record"]["id"] for record in records} == {"p-1", "p-2", "p-3"}
    assert set(ftp["downloads"]) == {
        "/imports/products-a.csv", "/imports/products-b.csv", "/imports/archive/products-c.csv",
    }


def test_missing_exact_file_does_not_advance_state(ftp, capsys):
    config = exact_config()
    catalog = selected_catalog(config)
    del ftp["files"]["/imports/inventory[1].csv"]
    state = {"bookmarks": {"products": {"modified_since": "2026-11-19T00:00:00+00:00"}}}
    previous = json.loads(json.dumps(state))
    ftp["downloads"].clear()

    tap.do_sync(config, catalog, state)
    assert not any(message["type"] == "RECORD" for message in messages(capsys))
    assert state == previous
    assert ftp["downloads"] == []


def test_failed_exact_file_read_does_not_advance_state(ftp, capsys):
    config = exact_config()
    catalog = selected_catalog(config)
    state = {"bookmarks": {"products": {"modified_since": "2026-11-19T00:00:00+00:00"}}}
    previous = json.loads(json.dumps(state))
    ftp["files"]["/imports/inventory[1].csv"] = b'id,name\np-1,"unclosed'
    config["tables"][0]["key_properties"] = ["missing_required_header"]

    with pytest.raises(Exception, match="CSV file missing required headers"):
        tap.do_sync(config, catalog, state)
    assert state == previous
    assert not any(message["type"] == "RECORD" for message in messages(capsys))


def test_nonrecursive_listing_does_not_return_directories(ftp):
    ftp["files"] = {"/imports/inventory[1].csv/child.csv": b"id\np-1\n"}
    config = exact_config()
    table = client.resolve_table_spec(config["tables"][0], config)
    conn = client.connection(config)

    assert conn.get_files(table["search_prefix"], table["search_pattern"], search_subdirectories=False) == []
    assert ftp["listings"] == ["/imports"]


@pytest.mark.parametrize("file_name", ["../inventory.csv", "nested/inventory.csv", "nested\\inventory.csv",
                                       ".", "..", " ", "inventory.csv\n", "inventory.csv\0", 42])
def test_invalid_exact_filenames_are_rejected(file_name):
    config = exact_config(file_name=file_name)
    with pytest.raises(ValueError, match="file_name"):
        client.resolve_table_spec(config["tables"][0], config)


@pytest.mark.parametrize("folder", [None, "", " ", 42])
def test_exact_filename_requires_shared_directory(folder):
    config = exact_config()
    config["files_path"] = folder
    with pytest.raises(ValueError, match="files_path"):
        client.resolve_table_spec(config["tables"][0], config)


@pytest.mark.parametrize("options", [{}, {"file_name": ""}, {"search_pattern": ""}, {"search_pattern": None}])
def test_missing_file_selector_fails_validation(options):
    with pytest.raises(ValueError, match="file_name or search_pattern"):
        client.resolve_table_spec({"table_name": "products", **options}, {})
