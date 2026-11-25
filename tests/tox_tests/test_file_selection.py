import csv
import gzip
import io
import json
import socket
import zipfile
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


@pytest.mark.parametrize("catalog_kind", ["missing", "empty", "stale"])
@pytest.mark.parametrize("new_stream", ["stocks", "warehouse_levels"])
def test_flat_sync_emits_configured_streams_and_headers_without_catalog_limits(
        ftp, tmp_path, monkeypatch, capsys, catalog_kind, new_stream):
    ftp["files"] = {
        "/imports/inventory[1].csv": b"id;naam\n0001;Item\n",
        "/imports/levels.csv": "product ref;available qty;Custom/仓库 (A)\n0001;0012;Depot\n".encode("utf-8"),
    }
    config = {
        "host": "synthetic.invalid", "port": 21, "username": "synthetic", "password": "synthetic",
        "files_path": "/imports", "start_date": "2000-01-01T00:00:00Z",
        "products_file_name": "inventory[1].csv", f"{new_stream}_file_name": "levels.csv",
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))
    argv = ["tap-ftp", "--config", str(config_path)]
    if catalog_kind != "missing":
        catalog = {"streams": []}
        if catalog_kind == "stale":
            catalog["streams"] = [{
                "stream": "products", "tap_stream_id": "products",
                "schema": {"type": "object", "properties": {"obsolete": {"type": "string"}}},
                "metadata": [{"breadcrumb": [], "metadata": {"selected": False}}],
            }, {
                "stream": "unconfigured", "tap_stream_id": "unconfigured", "schema": {},
                "metadata": [{"breadcrumb": [], "metadata": {"selected": True}}],
            }]
        catalog_path = tmp_path / "catalog.json"
        catalog_path.write_text(json.dumps(catalog))
        argv += ["--catalog", str(catalog_path)]
    monkeypatch.setattr("sys.argv", argv)
    tap.main()
    output = messages(capsys)
    schemas = {message["stream"]: message["schema"] for message in output if message["type"] == "SCHEMA"}
    records = {message["stream"]: message["record"] for message in output if message["type"] == "RECORD"}
    assert set(schemas) == set(records) == {"products", new_stream}
    assert {"id", "naam"} <= set(schemas["products"]["properties"])
    expected = {"product ref": "0001", "available qty": "0012", "Custom/仓库 (A)": "Depot"}
    assert set(schemas[new_stream]["properties"]) == set(expected) | {
        "_sdc_source_file", "_sdc_source_lineno", "_sdc_extra",
    }
    exported = io.StringIO()
    writer = csv.DictWriter(exported, fieldnames=list(schemas[new_stream]["properties"]))
    writer.writeheader()
    writer.writerow(records[new_stream])
    restored = next(csv.DictReader(io.StringIO(exported.getvalue())))
    assert {key: restored[key] for key in expected} == expected
    assert records["products"]["id"] == "0001"
    for position, message in enumerate(output):
        if message["type"] == "RECORD":
            assert any(previous["type"] == "SCHEMA" and previous["stream"] == message["stream"]
                       for previous in output[:position])
    assert sorted(ftp["downloads"]) == ["/imports/inventory[1].csv", "/imports/levels.csv"]


def test_flat_sync_refreshes_columns_after_file_headers_change(ftp, capsys):
    config = {
        "files_path": "/imports", "start_date": "2000-01-01T00:00:00Z",
        "products_file_name": "inventory[1].csv",
    }
    tap._configure_tables(config)
    catalog = selected_catalog(config)
    state = {}
    tap.do_sync(config, catalog, state)
    messages(capsys)
    ftp["modified"] = "20261122120000"
    ftp["files"]["/imports/inventory[1].csv"] = b"id;New column!\n0009;00042\n"
    ftp["downloads"].clear()
    tap.do_sync(config, catalog, state)
    output = messages(capsys)
    schema = next(message["schema"] for message in output if message["type"] == "SCHEMA")
    assert "New column!" in schema["properties"]
    assert "name" not in schema["properties"]
    record = next(message["record"] for message in output if message["type"] == "RECORD")
    assert record["id"] == "0009"
    assert record["New column!"] == "00042"
    assert ftp["downloads"] == ["/imports/inventory[1].csv"]


def test_flat_missing_file_is_excluded_until_uploaded_without_losing_state(ftp, capsys):
    config = {
        "files_path": "/imports", "start_date": "2000-01-01T00:00:00Z",
        "products_file_name": "inventory[1].csv", "stocks_file_name": "levels.csv",
    }
    tap._configure_tables(config)
    catalog = selected_catalog(config)
    assert [stream.stream for stream in catalog.streams] == ["products"]
    previous = {"modified_since": "2026-11-19T00:00:00+00:00"}
    state = {"bookmarks": {"stocks": dict(previous)}}
    ftp["downloads"].clear()
    tap.do_sync(config, catalog, state)
    first = messages(capsys)
    assert not any(message.get("stream") == "stocks" for message in first)
    assert state["bookmarks"]["stocks"] == previous
    assert ftp["downloads"] == ["/imports/inventory[1].csv"]
    ftp["files"]["/imports/levels.csv"] = b"product_id;quantity\n0001;0012\n"
    ftp["downloads"].clear()
    tap.do_sync(config, catalog, state)
    output = messages(capsys)
    records = [message for message in output if message["type"] == "RECORD"]
    assert len(records) == 1
    assert records[0]["stream"] == "stocks"
    assert records[0]["record"]["product_id"] == "0001"
    assert records[0]["record"]["quantity"] == "0012"
    assert state["bookmarks"]["stocks"]["modified_since"] == "2026-11-21T12:00:00+00:00"
    assert ftp["downloads"] == ["/imports/levels.csv"]


def test_flat_header_only_file_emits_its_actual_schema(ftp, capsys):
    ftp["files"]["/imports/inventory[1].csv"] = b"code;New field\n"
    config = {
        "files_path": "/imports", "start_date": "2000-01-01T00:00:00Z",
        "products_file_name": "inventory[1].csv",
    }
    tap._configure_tables(config)
    tap.do_sync(config, singer.Catalog.from_dict({"streams": []}), {})
    output = messages(capsys)
    schema = next(message["schema"] for message in output if message["type"] == "SCHEMA")
    assert {"code", "New field"} <= set(schema["properties"])
    assert not any(message["type"] == "RECORD" for message in output)


@pytest.mark.parametrize("headers", [
    ["id", "id"], ["id", "_sdc_source_file"], ["id", "_sdc_source_lineno"], ["id", "_sdc_extra"],
])
def test_flat_headers_cannot_silently_overwrite_values(ftp, capsys, headers):
    text = io.StringIO()
    writer = csv.writer(text)
    writer.writerow(headers)
    writer.writerow(["synthetic-one", "synthetic-two"])
    ftp["files"]["/imports/inventory[1].csv"] = text.getvalue().encode("utf-8")
    config = {
        "files_path": "/imports", "start_date": "2000-01-01T00:00:00Z",
        "products_file_name": "inventory[1].csv",
    }
    tap._configure_tables(config)
    state = {"bookmarks": {"products": {"modified_since": "2026-11-19T00:00:00+00:00"}}}
    previous = json.loads(json.dumps(state))
    with pytest.raises(ValueError, match="unique.*reserved"):
        tap.do_sync(config, singer.Catalog.from_dict({"streams": []}), state)
    assert state == previous
    assert not any(message["type"] in ("SCHEMA", "RECORD") for message in messages(capsys))


def test_legacy_catalog_still_controls_stream_selection(ftp, capsys):
    config = exact_config()
    catalog = selected_catalog(config)
    for item in catalog.streams[0].metadata:
        if not item["breadcrumb"]:
            item["metadata"]["selected"] = False
    ftp["downloads"].clear()
    state = {}
    tap.do_sync(config, catalog, state)
    assert not any(message["type"] in ("SCHEMA", "RECORD") for message in messages(capsys))
    assert ftp["downloads"] == []
    assert state == {}


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


@pytest.fixture
def file_job(tmp_path, monkeypatch):
    config = {
        "host": "synthetic.invalid", "port": 21, "username": "synthetic", "password": "synthetic",
        "files_path": "/imports", "start_date": "2000-01-01T00:00:00Z",
        "products_file_name": "inventory[1].csv",
    }
    config_path = tmp_path / "config.json"
    invocation = 0

    def run(*arguments):
        nonlocal invocation
        invocation += 1
        output = tmp_path / f"sync-output-{invocation}"
        config["target_dir"] = str(output)
        config_path.write_text(json.dumps(config))
        monkeypatch.setattr("sys.argv", ["tap-ftp", "--config", str(config_path), *arguments])
        tap.main()
        return output

    return config, run


def output_rows(directory, stream):
    with (directory / f"{stream}.csv").open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return reader.fieldnames, list(reader)


@pytest.mark.parametrize("delimiter", [",", ";", "\t", "|"])
@pytest.mark.parametrize("stream", ["stocks", "warehouse_levels"])
def test_direct_files_preserve_all_columns_without_catalog_filtering(
        ftp, file_job, tmp_path, capsys, delimiter, stream):
    config, run = file_job
    config[f"{stream}_file_name"] = "levels.csv"
    name = "Widget, semi; tab\tpipe| with\nnotes"
    text = io.StringIO()
    writer = csv.writer(text, delimiter=delimiter)
    writer.writerow(["product ref", "Custom/仓库 (A)", "new column"])
    writer.writerow(["0001", name, "00042"])
    ftp["files"]["/imports/levels.csv"] = text.getvalue().encode()
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(json.dumps({"streams": [{
        "stream": stream, "tap_stream_id": stream,
        "schema": {"properties": {"obsolete": {"type": "string"}}},
        "metadata": [{"breadcrumb": [], "metadata": {"selected": False}}],
    }]}))
    output = run("--catalog", str(catalog_path))
    headers, rows = output_rows(output, stream)
    assert headers == [
        "product ref", "Custom/仓库 (A)", "new column",
        "_sdc_source_file", "_sdc_source_lineno", "_sdc_extra",
    ]
    assert rows == [{
        "product ref": "0001", "Custom/仓库 (A)": name, "new column": "00042",
        "_sdc_source_file": "/imports/levels.csv", "_sdc_source_lineno": "2", "_sdc_extra": "[]",
    }]
    assert [row["id"] for row in output_rows(output, "products")[1]] == ["p-1", "p-2"]
    assert capsys.readouterr().out == ""
    assert sorted(ftp["downloads"]) == ["/imports/inventory[1].csv", "/imports/levels.csv"]


def test_direct_files_resume_adjacent_state_and_keep_file_and_row_bookmarks(
        ftp, file_job, tmp_path, capsys):
    config, run = file_job
    config["products_replication_key_column"] = "updated_at"
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps({"unrelated": "keep", "bookmarks": {"other": {"cursor": "keep"}}}))
    first = run()
    assert [row["id"] for row in output_rows(first, "products")[1]] == ["p-1", "p-2"]
    checkpoint = json.loads(state_path.read_text())
    assert checkpoint["unrelated"] == "keep"
    assert checkpoint["bookmarks"]["other"] == {"cursor": "keep"}
    assert checkpoint["bookmarks"]["products"] == {
        "modified_since": "2026-11-21T12:00:00+00:00",
        "replication_key_column": "updated_at",
        "replication_key_value": "2026-11-20T13:00:00+00:00",
    }
    ftp["downloads"].clear()
    unchanged = run()
    assert list(unchanged.glob("*.csv")) == []
    assert ftp["downloads"] == []
    assert json.loads(state_path.read_text()) == checkpoint
    ftp["modified"] = "20261122120000"
    ftp["files"]["/imports/inventory[1].csv"] = (
        b"id;name;updated_at;new column\n"
        b"p-1;Widget;2026-11-20T12:00:00Z;old\n"
        b"p-2;Gadget;2026-11-20T13:00:00Z;old\n"
        b"p-3;New;2026-11-22T12:00:00Z;00042\n"
    )
    updated = run()
    headers, rows = output_rows(updated, "products")
    assert "new column" in headers
    assert [(row["id"], row["new column"]) for row in rows] == [("p-3", "00042")]
    assert json.loads(state_path.read_text())["bookmarks"]["products"]["replication_key_value"] == (
        "2026-11-22T12:00:00+00:00"
    )
    assert capsys.readouterr().out == ""


def test_direct_files_honor_explicit_state_path(ftp, file_job, tmp_path):
    _, run = file_job
    adjacent_state = tmp_path / "state.json"
    adjacent_state.write_text('{"keep": "adjacent"}')
    explicit_state = tmp_path / "explicit-state.json"
    explicit_state.write_text(json.dumps({
        "keep": "explicit", "bookmarks": {"products": {"modified_since": "2026-11-21T12:00:00+00:00"}},
    }))
    output = run("--state", str(explicit_state))
    assert list(output.glob("*.csv")) == []
    assert ftp["downloads"] == []
    assert json.loads(adjacent_state.read_text()) == {"keep": "adjacent"}
    assert json.loads(explicit_state.read_text())["keep"] == "explicit"


def test_direct_files_skip_missing_files_without_losing_their_bookmarks(ftp, file_job, tmp_path):
    config, run = file_job
    config["stocks_file_name"] = "levels.csv"
    previous = {"modified_since": "2026-11-19T00:00:00+00:00"}
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps({"bookmarks": {"stocks": previous}}))
    output = run()
    assert not (output / "stocks.csv").exists()
    assert json.loads(state_path.read_text())["bookmarks"]["stocks"] == previous
    ftp["files"]["/imports/levels.csv"] = b"product_id;quantity\n0001;0012\n"
    ftp["downloads"].clear()
    output = run()
    assert [row["quantity"] for row in output_rows(output, "stocks")[1]] == ["0012"]
    assert ftp["downloads"] == ["/imports/levels.csv"]


@pytest.mark.parametrize("contents", [
    b"id,name;code\np-1,Widget;A\n",
    b"id;id\np-1;p-2\n",
    b"id;_sdc_extra\np-1;value\n",
])
def test_direct_file_failure_does_not_publish_partial_csv_or_advance_state(
        ftp, file_job, tmp_path, contents):
    config, run = file_job
    config["suppliers_file_name"] = "vendors.csv"
    ftp["files"]["/imports/vendors.csv"] = contents
    state_path = tmp_path / "state.json"
    previous = '{"keep": "original bytes", "bookmarks": {}}'
    state_path.write_text(previous)
    with pytest.raises(ValueError):
        run()
    assert state_path.read_text() == previous
    assert not list((tmp_path / "sync-output-1").glob("*suppliers*"))
    ftp["files"]["/imports/vendors.csv"] = b"id;name\ns-1;Vendor\n"
    output = run()
    assert [row["id"] for row in output_rows(output, "products")[1]] == ["p-1", "p-2"]
    assert [row["id"] for row in output_rows(output, "suppliers")[1]] == ["s-1"]


@pytest.mark.parametrize("saved", ['{"bookmarks": []}', '[]', '{broken'])
def test_direct_files_reject_malformed_state_before_source_reads(ftp, file_job, tmp_path, saved):
    _, run = file_job
    (tmp_path / "state.json").write_text(saved)
    with pytest.raises(ValueError):
        run()
    assert ftp["listings"] == []
    assert ftp["downloads"] == []
    assert (tmp_path / "state.json").read_text() == saved


def test_direct_file_state_write_failure_is_retryable(ftp, file_job, tmp_path, monkeypatch):
    from tap_ftp import file_output

    _, run = file_job
    state_path = tmp_path / "state.json"
    previous = '{"bookmarks": {}}'
    state_path.write_text(previous)
    replace = file_output.os.replace

    def fail_state(source, destination):
        if destination == state_path:
            raise OSError("Synthetic state write failure")
        replace(source, destination)

    with monkeypatch.context() as failure:
        failure.setattr(file_output.os, "replace", fail_state)
        with pytest.raises(OSError, match="state write failure"):
            run()
    assert state_path.read_text() == previous
    assert not list(tmp_path.glob(".*.tmp"))
    output = run()
    assert [row["id"] for row in output_rows(output, "products")[1]] == ["p-1", "p-2"]


def test_direct_header_only_file_preserves_its_headers(ftp, file_job):
    ftp["files"]["/imports/inventory[1].csv"] = b"id;new column\n"
    _, run = file_job
    headers, rows = output_rows(run(), "products")
    assert headers[:2] == ["id", "new column"]
    assert rows == []


@pytest.mark.parametrize("extension", ["gz", "zip"])
def test_direct_files_keep_supported_csv_compression(ftp, file_job, extension):
    config, run = file_job
    filename = f"inventory.csv.{extension}"
    config["products_file_name"] = filename
    contents = b"id;name\n0001;Item\n"
    if extension == "gz":
        packed = gzip.compress(contents)
    else:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("first.csv", contents)
            archive.writestr("second.csv", b"id;name\n0002;Other\n")
        packed = buffer.getvalue()
    ftp["files"][f"/imports/{filename}"] = packed
    rows = output_rows(run(), "products")[1]
    assert [row["id"] for row in rows] == (["0001"] if extension == "gz" else ["0001", "0002"])


def test_direct_zip_header_mismatch_does_not_publish_partial_data(ftp, file_job, tmp_path):
    config, run = file_job
    config["products_file_name"] = "inventory.zip"
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("first.csv", b"id;name\n0001;Item\n")
        archive.writestr("second.csv", b"id;renamed\n0002;Other\n")
    ftp["files"]["/imports/inventory.zip"] = buffer.getvalue()
    with pytest.raises(ValueError, match="same headers"):
        run()
    assert not (tmp_path / "sync-output-1" / "products.csv").exists()
    assert not (tmp_path / "state.json").exists()


def test_direct_files_reject_reusing_a_previous_job_output(ftp, file_job, tmp_path):
    _, run = file_job
    output = run()
    previous = (output / "products.csv").read_bytes()
    checkpoint = (tmp_path / "state.json").read_bytes()
    ftp["downloads"].clear()
    with pytest.raises(ValueError, match="fresh job output"):
        tap.main()
    assert ftp["downloads"] == []
    assert (output / "products.csv").read_bytes() == previous
    assert (tmp_path / "state.json").read_bytes() == checkpoint


@pytest.mark.parametrize("target_dir", [None, "", " ", 42])
def test_direct_files_reject_invalid_output_directory(ftp, file_job, tmp_path, monkeypatch, target_dir):
    config, _ = file_job
    config["target_dir"] = target_dir
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))
    monkeypatch.setattr("sys.argv", ["tap-ftp", "--config", str(config_path)])
    with pytest.raises(ValueError, match="target_dir"):
        tap.main()
    assert ftp["listings"] == []


def test_direct_empty_zip_does_not_publish_or_advance_state(ftp, file_job, tmp_path):
    config, run = file_job
    config["products_file_name"] = "empty.zip"
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w"):
        pass
    ftp["files"]["/imports/empty.zip"] = buffer.getvalue()
    state_path = tmp_path / "state.json"
    previous = '{"bookmarks": {"other": {"cursor": "keep"}}}'
    state_path.write_text(previous)
    with pytest.raises(ValueError, match="no CSV members"):
        run()
    assert state_path.read_text() == previous
    assert list((tmp_path / "sync-output-1").iterdir()) == []


def test_direct_files_reject_remote_deletion_before_reading_source(ftp, file_job):
    config, run = file_job
    config["delete_after_sync"] = True
    with pytest.raises(ValueError, match="cannot delete source files"):
        run()
    assert ftp["listings"] == []
    assert ftp["downloads"] == []


def test_direct_files_reject_stale_unconfigured_csvs(ftp, file_job, tmp_path):
    _, run = file_job
    output = run()
    (output / "products.csv").rename(output / "unconfigured.csv")
    previous = (output / "unconfigured.csv").read_bytes()
    ftp["downloads"].clear()
    with pytest.raises(ValueError, match="fresh job output"):
        tap.main()
    assert ftp["downloads"] == []
    assert (output / "unconfigured.csv").read_bytes() == previous
