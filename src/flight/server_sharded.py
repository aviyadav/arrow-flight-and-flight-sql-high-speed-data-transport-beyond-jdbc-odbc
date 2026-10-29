from __future__ import annotations

import argparse
import json

import pyarrow.flight as flight

from src.config import Settings
from src.flight.common import get_result_schema, make_location
from src.flight.server_minimal import METADATA_TYPES, MinimalPostgresFlightServer


class ShardedPostgresFlightServer(MinimalPostgresFlightServer):
    def __init__(
        self,
        settings: Settings,
        shard_count: int = 4,
        worker_uris: list[str] | None = None,
        location: str | None = None,
        advertised_location: str | None = None,
        batch_size: int | None = None,
    ):
        super().__init__(
            settings,
            location=location,
            advertised_location=advertised_location,
            batch_size=batch_size,
        )
        self.shard_count = shard_count
        self.worker_uris = worker_uris or []

    def get_flight_info(self, context, descriptor):
        if descriptor.command:
            text = descriptor.command.decode()
            if text.lstrip().startswith("{"):
                cmd = json.loads(text)
                typ = cmd.get("type")
                if typ in METADATA_TYPES or typ == "prepared_execute":
                    return super().get_flight_info(context, descriptor)

        base_sql = self._resolve_base_sql(descriptor).strip().rstrip(";")
        schema = get_result_schema(self.settings, base_sql)

        endpoints = []
        for i in range(self.shard_count):
            partition_sql = (
                f"SELECT * FROM ({base_sql}) AS _flight_part "
                f"WHERE mod(id, {self.shard_count}) = {i}"
            )
            ticket = json.dumps(
                {
                    "type": "partition_sql",
                    "sql": partition_sql,
                    "partition": i,
                }
            ).encode()

            loc = (
                self.worker_uris[i % len(self.worker_uris)]
                if self.worker_uris
                else self._loc
            )

            endpoints.append(
                flight.FlightEndpoint(
                    flight.Ticket(ticket),
                    [make_location(loc)],
                )
            )

        return flight.FlightInfo(
            schema=schema,
            descriptor=descriptor,
            endpoints=endpoints,
            total_records=-1,
            total_bytes=-1,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Sharded / distributed Arrow Flight server")
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--host", default=None)
    parser.add_argument("--advertised-uri", default=None)
    parser.add_argument("--shards", type=int, default=4)
    parser.add_argument("--workers", nargs="*", default=[])
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

    server = ShardedPostgresFlightServer(
        settings,
        shard_count=args.shards,
        worker_uris=args.workers,
        advertised_location=args.advertised_uri,
        batch_size=args.batch_size,
    )

    print(f"Serving sharded Arrow Flight on {settings.flight_listen_location}")
    print(f"Advertised client location: {server._loc}")
    print(f"Shards: {args.shards}")
    print(f"Workers: {args.workers or ['self']}")

    server.serve()


if __name__ == "__main__":
    main()