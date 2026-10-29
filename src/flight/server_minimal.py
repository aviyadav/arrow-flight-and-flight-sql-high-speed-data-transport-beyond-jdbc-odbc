from __future__ import annotations

import argparse
import json
import threading
import uuid

import pyarrow.flight as flight

from src.config import Settings
from src.db.postgres import insert_record_batches
from src.flight.auth import BearerAuthFactory
from src.flight.common import get_result_schema, make_location, make_query_stream

DATASETS = {
    "events": "SELECT id, day, ts, user_id, kind, payload, amount FROM events",
    "sales": "SELECT id, region, amount, ts, day FROM sales",
}

METADATA_TYPES = {
    "get_catalogs",
    "get_db_schemas",
    "get_tables",
}


def _tls_certificates(settings: Settings):
    if not settings.flight_tls:
        return None

    with open(settings.flight_cert, "rb") as f:
        cert = f.read()
    with open(settings.flight_key, "rb") as f:
        key = f.read()

    return [(cert, key)]


class MinimalPostgresFlightServer(flight.FlightServerBase):
    def __init__(
        self,
        settings: Settings,
        location: str | None = None,
        advertised_location: str | None = None,
        batch_size: int | None = None,
    ):
        listen_location = location or settings.flight_listen_location
        middleware = None
        if settings.flight_auth_token:
            middleware = {"auth": BearerAuthFactory(settings.flight_auth_token)}

        super().__init__(
            location=listen_location,
            middleware=middleware,
            tls_certificates=_tls_certificates(settings),
        )

        self.settings = settings
        self._loc = advertised_location or settings.flight_client_location
        self.batch_size = batch_size or settings.batch_size

        self._lock = threading.Lock()
        self._staging: dict[str, list] = {}
        self._prepared: dict[str, str] = {}
        self._prepared_params: dict[str, tuple] = {}
        self._transactions: set[str] = set()

    def _metadata_sql(self, kind: str) -> str:
        if kind == "get_catalogs":
            return "SELECT current_database() AS catalog_name"

        if kind == "get_db_schemas":
            return """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                ORDER BY schema_name
            """

        if kind == "get_tables":
            safe_schema = self.settings.pg_schema.replace("'", "''")
            return f"""
                SELECT
                    table_catalog,
                    table_schema,
                    table_name,
                    table_type
                FROM information_schema.tables
                WHERE table_schema = '{safe_schema}'
                ORDER BY table_name
            """

        raise ValueError(f"Unknown metadata type: {kind}")

    def _resolve_base_sql(self, descriptor: flight.FlightDescriptor) -> str:
        if descriptor.command:
            text = descriptor.command.decode()
            if text.lstrip().startswith("{"):
                obj = json.loads(text)
                typ = obj.get("type")
                if typ == "sql":
                    return obj["sql"]
                if typ == "prepared_execute":
                    return self._prepared[obj["handle"]]
                if typ in METADATA_TYPES:
                    return self._metadata_sql(typ)
            return text

        if descriptor.path:
            name = descriptor.path[0].decode()
            if name not in DATASETS:
                raise ValueError(f"Unknown dataset: {name}")
            return DATASETS[name]

        raise ValueError("Empty descriptor")

    def get_flight_info(self, context, descriptor):
        params = None

        if descriptor.command:
            text = descriptor.command.decode()

            if text.lstrip().startswith("{"):
                cmd = json.loads(text)
                typ = cmd.get("type")

                if typ == "prepared_execute":
                    handle = cmd["handle"]
                    sql_text = self._prepared[handle]
                    params = self._prepared_params.get(handle, ())
                    schema = get_result_schema(self.settings, sql_text, params)
                    ticket = json.dumps({"type": "prepared_execute", "handle": handle}).encode()
                    endpoint = flight.FlightEndpoint(
                        flight.Ticket(ticket),
                        [make_location(self._loc)],
                    )
                    return flight.FlightInfo(
                        schema=schema,
                        descriptor=descriptor,
                        endpoints=[endpoint],
                        total_records=-1,
                        total_bytes=-1,
                    )

                if typ in METADATA_TYPES:
                    sql_text = self._metadata_sql(typ)
                    schema = get_result_schema(self.settings, sql_text)
                    ticket = json.dumps({"type": "metadata", "kind": typ}).encode()
                    endpoint = flight.FlightEndpoint(
                        flight.Ticket(ticket),
                        [make_location(self._loc)],
                    )
                    return flight.FlightInfo(
                        schema=schema,
                        descriptor=descriptor,
                        endpoints=[endpoint],
                        total_records=-1,
                        total_bytes=-1,
                    )

                if typ == "sql":
                    sql_text = cmd["sql"]
                else:
                    sql_text = text
            else:
                sql_text = text

        elif descriptor.path:
            name = descriptor.path[0].decode()
            sql_text = DATASETS[name]

        else:
            raise ValueError("Empty descriptor")

        schema = get_result_schema(self.settings, sql_text)
        ticket = json.dumps({"type": "sql", "sql": sql_text}).encode()
        endpoint = flight.FlightEndpoint(
            flight.Ticket(ticket),
            [make_location(self._loc)],
        )

        return flight.FlightInfo(
            schema=schema,
            descriptor=descriptor,
            endpoints=[endpoint],
            total_records=-1,
            total_bytes=-1,
        )

    def do_get(self, context, ticket):
        payload = json.loads(ticket.ticket.decode())
        typ = payload.get("type")

        if typ == "prepared_execute":
            handle = payload["handle"]
            sql_text = self._prepared[handle]
            params = self._prepared_params.get(handle, ())
        elif typ == "metadata":
            sql_text = self._metadata_sql(payload["kind"])
            params = None
        elif typ in {"sql", "partition_sql"}:
            sql_text = payload["sql"]
            params = None
        else:
            raise ValueError(f"Unsupported ticket type: {typ}")

        schema, gen = make_query_stream(
            self.settings,
            sql_text,
            batch_size=self.batch_size,
            params=params,
        )
        return flight.GeneratorStream(schema, gen)

    def do_put(self, context, descriptor, reader, writer):
        if descriptor.command:
            text = descriptor.command.decode()
            if text.lstrip().startswith("{"):
                cmd = json.loads(text)
                if cmd.get("type") == "prepared_bind":
                    handle = cmd["handle"]
                    batches = [chunk.data for chunk in reader]
                    params = self._batch_to_params(batches[0]) if batches else ()
                    with self._lock:
                        self._prepared_params[handle] = params
                    writer.write(b"bound")
                    return

        name = descriptor.path[0].decode() if descriptor.path else "default"
        batches = [chunk.data for chunk in reader]

        with self._lock:
            self._staging[name] = batches

        meta = {
            "stored_batches": len(batches),
            "stored_rows": sum(b.num_rows for b in batches),
        }
        writer.write(json.dumps(meta).encode())

    def _batch_to_params(self, batch) -> tuple:
        if batch.num_rows == 0:
            return ()
        row = batch.to_pylist()[0]
        return tuple(row[field.name] for field in batch.schema)

    def do_action(self, context, action):
        body = action.body.to_pybytes() if action.body is not None else b""

        if action.type == "create_prepared":
            payload = json.loads(body.decode())
            handle = str(uuid.uuid4())
            with self._lock:
                self._prepared[handle] = payload["sql"]
            yield flight.Result(handle.encode())

        elif action.type == "close_prepared":
            handle = body.decode()
            with self._lock:
                self._prepared.pop(handle, None)
                self._prepared_params.pop(handle, None)
            yield flight.Result(b"closed")

        elif action.type == "begin_transaction":
            handle = str(uuid.uuid4())
            with self._lock:
                self._transactions.add(handle)
            yield flight.Result(handle.encode())

        elif action.type == "end_transaction":
            handle = body.decode()
            with self._lock:
                self._transactions.discard(handle)
            yield flight.Result(b"ended")

        elif action.type == "commit_ingest":
            payload = json.loads(body.decode())
            staging_name = payload.get("staging") or payload.get("table")
            target_table = payload.get("table") or staging_name

            with self._lock:
                batches = self._staging.pop(staging_name, [])

            rows = insert_record_batches(self.settings, target_table, batches)
            yield flight.Result(json.dumps({"committed_rows": rows}).encode())

        elif action.type == "list_staging":
            with self._lock:
                info = {
                    k: sum(b.num_rows for b in v)
                    for k, v in self._staging.items()
                }
            yield flight.Result(json.dumps(info).encode())

        else:
            raise ValueError(f"Unknown action: {action.type}")

    def list_actions(self, context):
        return [
            ("create_prepared", "Create a prepared statement"),
            ("close_prepared", "Close a prepared statement"),
            ("begin_transaction", "Begin a stub transaction"),
            ("end_transaction", "End a stub transaction"),
            ("commit_ingest", "Commit staged DoPut data into PostgreSQL"),
            ("list_staging", "List staged datasets"),
        ]

    def list_flights(self, context, criteria):
        for name, sql_text in DATASETS.items():
            schema = get_result_schema(self.settings, sql_text)
            descriptor = flight.FlightDescriptor.for_path(name)
            ticket = json.dumps({"type": "sql", "sql": sql_text}).encode()
            endpoint = flight.FlightEndpoint(
                flight.Ticket(ticket),
                [make_location(self._loc)],
            )
            yield flight.FlightInfo(
                schema=schema,
                descriptor=descriptor,
                endpoints=[endpoint],
                total_records=-1,
                total_bytes=-1,
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Minimal PostgreSQL-backed Arrow Flight server")
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--host", default=None)
    parser.add_argument("--advertised-uri", default=None)
    parser.add_argument("--tls", action="store_true")
    parser.add_argument("--cert", default=None)
    parser.add_argument("--key", default=None)
    parser.add_argument("--token", default=None)
    parser.add_argument("--no-auth", action="store_true")
    parser.add_argument("--batch-size", type=int, default=None)
    args = parser.parse_args()

    settings = Settings.load()

    overrides = {}
    if args.host:
        overrides["flight_host"] = args.host
    if args.port:
        overrides["flight_port"] = args.port
    if args.tls:
        overrides["flight_tls"] = True
    if args.cert:
        overrides["flight_cert"] = args.cert
    if args.key:
        overrides["flight_key"] = args.key
    if args.no_auth:
        overrides["flight_auth_token"] = ""
    elif args.token is not None:
        overrides["flight_auth_token"] = args.token
    if args.batch_size:
        overrides["batch_size"] = args.batch_size

    if overrides:
        settings = settings.replace(**overrides)

    server = MinimalPostgresFlightServer(
        settings,
        advertised_location=args.advertised_uri,
        batch_size=args.batch_size,
    )

    print(f"Serving Arrow Flight on {settings.flight_listen_location}")
    print(f"Advertised client location: {server._loc}")
    print(f"TLS: {settings.flight_tls}")
    print(f"Auth: {'enabled' if settings.flight_auth_token else 'disabled'}")

    server.serve()


if __name__ == "__main__":
    main()