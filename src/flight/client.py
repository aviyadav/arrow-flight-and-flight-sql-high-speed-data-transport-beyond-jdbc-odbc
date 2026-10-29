from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import date

import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.parquet as pq

from src.flight.auth import TokenClientMiddlewareFactory


def connect_client(uri: str, token: str | None = None, ca_path: str | None = None):
    middleware = [TokenClientMiddlewareFactory(token)] if token else None
    tls_root_certs = None

    # CA certs are only meaningful for TLS connections. For plaintext
    # (grpc://) connections, skip reading them so a stale/missing
    # FLIGHT_CA_PATH doesn't crash the client.
    if ca_path and uri.startswith("grpc+tls://"):
        with open(ca_path, "rb") as f:
            tls_root_certs = f.read()

    return flight.FlightClient(
        uri,
        tls_root_certs=tls_root_certs,
        middleware=middleware,
    )


def _endpoint_uri(ep, default_uri: str) -> str:
    if not ep.locations:
        return default_uri

    loc = ep.locations[0]
    uri = getattr(loc, "uri", loc)

    if isinstance(uri, bytes):
        return uri.decode()

    return str(uri)


def read_sql(uri: str, sql_text: str, token: str | None = None, ca_path: str | None = None) -> pa.Table:
    client = connect_client(uri, token=token, ca_path=ca_path)
    descriptor = flight.FlightDescriptor.for_command(sql_text.encode())
    info = client.get_flight_info(descriptor)
    return client.do_get(info.endpoints[0].ticket).read_all()


def read_path(uri: str, path: str, token: str | None = None, ca_path: str | None = None) -> pa.Table:
    client = connect_client(uri, token=token, ca_path=ca_path)
    descriptor = flight.FlightDescriptor.for_path(path)
    info = client.get_flight_info(descriptor)
    return client.do_get(info.endpoints[0].ticket).read_all()


def read_metadata(uri: str, kind: str, token: str | None = None, ca_path: str | None = None) -> pa.Table:
    client = connect_client(uri, token=token, ca_path=ca_path)
    cmd = json.dumps({"type": kind}).encode()
    descriptor = flight.FlightDescriptor.for_command(cmd)
    info = client.get_flight_info(descriptor)
    return client.do_get(info.endpoints[0].ticket).read_all()


def read_parallel_uri(
    uri: str,
    sql_text: str,
    token: str | None = None,
    ca_path: str | None = None,
    max_workers: int | None = None,
) -> pa.Table:
    client = connect_client(uri, token=token, ca_path=ca_path)
    descriptor = flight.FlightDescriptor.for_command(sql_text.encode())
    info = client.get_flight_info(descriptor)

    if not info.endpoints:
        return pa.table([], schema=info.schema)

    def read_endpoint(ep):
        ep_uri = _endpoint_uri(ep, uri)
        c = connect_client(ep_uri, token=token, ca_path=ca_path)
        return c.do_get(ep.ticket).read_all()

    workers = max_workers or len(info.endpoints)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        parts = list(pool.map(read_endpoint, info.endpoints))

    return pa.concat_tables(parts)


def put_table(uri: str, path: str, table: pa.Table, token: str | None = None, ca_path: str | None = None):
    client = connect_client(uri, token=token, ca_path=ca_path)
    descriptor = flight.FlightDescriptor.for_path(path)
    writer, _ = client.do_put(descriptor, table.schema)
    writer.write_table(table)
    writer.close()


def commit_ingest(
    uri: str,
    staging_name: str,
    target_table: str,
    token: str | None = None,
    ca_path: str | None = None,
):
    client = connect_client(uri, token=token, ca_path=ca_path)
    body = json.dumps({"staging": staging_name, "table": target_table}).encode()
    action = flight.Action("commit_ingest", body)
    results = list(client.do_action(action))
    if not results:
        return {}
    return json.loads(results[0].body.to_pybytes().decode())


def list_staging(uri: str, token: str | None = None, ca_path: str | None = None):
    client = connect_client(uri, token=token, ca_path=ca_path)
    action = flight.Action("list_staging", b"")
    results = list(client.do_action(action))
    if not results:
        return {}
    return json.loads(results[0].body.to_pybytes().decode())


def prepared_demo(uri: str, token: str | None = None, ca_path: str | None = None) -> pa.Table:
    client = connect_client(uri, token=token, ca_path=ca_path)

    sql_text = "SELECT id, kind, amount FROM events WHERE day=%s AND kind=%s"

    create_action = flight.Action(
        "create_prepared",
        json.dumps({"sql": sql_text}).encode(),
    )
    create_results = list(client.do_action(create_action))
    handle = create_results[0].body.to_pybytes().decode()

    params_batch = pa.record_batch(
        [
            pa.array([date(2026, 8, 1)], type=pa.date32()),
            pa.array(["purchase"], type=pa.string()),
        ],
        names=["day", "kind"],
    )

    bind_descriptor = flight.FlightDescriptor.for_command(
        json.dumps({"type": "prepared_bind", "handle": handle}).encode()
    )
    writer, _ = client.do_put(bind_descriptor, params_batch.schema)
    writer.write_batch(params_batch)
    writer.close()

    exec_descriptor = flight.FlightDescriptor.for_command(
        json.dumps({"type": "prepared_execute", "handle": handle}).encode()
    )
    info = client.get_flight_info(exec_descriptor)
    table = client.do_get(info.endpoints[0].ticket).read_all()

    close_action = flight.Action("close_prepared", handle.encode())
    list(client.do_action(close_action))

    return table


def main() -> None:
    parser = argparse.ArgumentParser(description="Arrow Flight client examples")
    parser.add_argument("--uri", default="grpc://localhost:8815")
    parser.add_argument("--token", default=None)
    parser.add_argument("--ca", default=None)

    parser.add_argument("--sql", default=None)
    parser.add_argument("--path", default=None)
    parser.add_argument("--metadata", default=None, choices=[
        "get_catalogs",
        "get_db_schemas",
        "get_tables",
    ])

    parser.add_argument("--parallel", action="store_true")
    parser.add_argument("--max-workers", type=int, default=None)

    parser.add_argument("--prepared-demo", action="store_true")
    parser.add_argument("--list-flights", action="store_true")
    parser.add_argument("--list-staging", action="store_true")

    parser.add_argument("--put-parquet", default=None)
    parser.add_argument("--put-path", default="upload_demo")
    parser.add_argument("--put-rows", type=int, default=10_000)
    parser.add_argument("--commit-table", default=None)

    args = parser.parse_args()

    if args.prepared_demo:
        table = prepared_demo(args.uri, token=args.token, ca_path=args.ca)
        print("Prepared demo rows:", table.num_rows)
        print(table.slice(0, min(10, table.num_rows)).to_pydict())
        return

    if args.list_flights:
        client = connect_client(args.uri, token=args.token, ca_path=args.ca)
        for info in client.list_flights():
            print(info.descriptor, info.schema, info.total_records)
        return

    if args.list_staging:
        print(json.dumps(list_staging(args.uri, token=args.token, ca_path=args.ca), indent=2))
        return

    if args.put_parquet:
        table = pq.read_table(args.put_parquet)
        if args.put_rows > 0:
            table = table.slice(0, args.put_rows)

        put_table(
            args.uri,
            args.put_path,
            table,
            token=args.token,
            ca_path=args.ca,
        )
        print(f"Put {table.num_rows} rows to staging path={args.put_path}")

        if args.commit_table:
            result = commit_ingest(
                args.uri,
                staging_name=args.put_path,
                target_table=args.commit_table,
                token=args.token,
                ca_path=args.ca,
            )
            print("Commit result:", result)
        return

    if args.metadata:
        table = read_metadata(args.uri, args.metadata, token=args.token, ca_path=args.ca)
        print(table.to_pydict())
        return

    if args.path:
        table = read_path(args.uri, args.path, token=args.token, ca_path=args.ca)
        print("Rows:", table.num_rows)
        print(table.schema)
        print(table.slice(0, min(10, table.num_rows)).to_pydict())
        return

    if args.sql:
        if args.parallel:
            table = read_parallel_uri(
                args.uri,
                args.sql,
                token=args.token,
                ca_path=args.ca,
                max_workers=args.max_workers,
            )
        else:
            table = read_sql(args.uri, args.sql, token=args.token, ca_path=args.ca)

        print("Rows:", table.num_rows)
        print(table.schema)
        print(table.slice(0, min(10, table.num_rows)).to_pydict())
        return

    parser.print_help()


if __name__ == "__main__":
    main()