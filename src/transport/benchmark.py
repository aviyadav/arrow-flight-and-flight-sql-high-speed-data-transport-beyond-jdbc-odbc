from __future__ import annotations

import argparse
import os
import time

import pandas as pd

from src.config import Settings
from src.db.postgres import connect, explain_analyze_time_ms
from src.flight.client import connect_client, read_parallel_uri


def direct_pg_fetch(
    settings: Settings,
    sql_text: str,
    fetch_size: int,
    materialize: bool = True,
):
    conn = connect(settings)
    cur = conn.cursor()

    t0 = time.perf_counter()
    cur.execute(sql_text)
    cols = [d.name for d in cur.description]

    n = 0
    frames = []

    while True:
        rows = cur.fetchmany(fetch_size)
        if not rows:
            break
        n += len(rows)
        if materialize:
            frames.append(pd.DataFrame(rows, columns=cols))

    if materialize and frames:
        df = pd.concat(frames, ignore_index=True)
        n = len(df)
    elif materialize:
        df = pd.DataFrame(columns=cols)
    else:
        df = None

    dt = time.perf_counter() - t0
    conn.close()

    return n, dt, df


def flight_fetch(
    uri: str,
    sql_text: str,
    token: str | None = None,
    ca_path: str | None = None,
    parallel: bool = False,
):
    t0 = time.perf_counter()

    if parallel:
        table = read_parallel_uri(uri, sql_text, token=token, ca_path=ca_path)
    else:
        client = connect_client(uri, token=token, ca_path=ca_path)
        from pyarrow import flight as pa_flight

        descriptor = pa_flight.FlightDescriptor.for_command(sql_text.encode())
        info = client.get_flight_info(descriptor)
        table = client.do_get(info.endpoints[0].ticket).read_all()

    dt = time.perf_counter() - t0
    return table.num_rows, dt, table


def main() -> None:
    parser = argparse.ArgumentParser(description="Measure PostgreSQL row transport vs Arrow Flight transport")
    parser.add_argument(
        "--query",
        default="SELECT * FROM events WHERE day='2026-08-01'",
    )
    parser.add_argument("--flight-uri", default=os.getenv("FLIGHT_URI", "grpc://localhost:8815"))
    parser.add_argument("--flight-token", default=os.getenv("FLIGHT_AUTH_TOKEN", "") or None)
    parser.add_argument("--flight-ca", default=os.getenv("FLIGHT_CA_PATH", "") or None)
    parser.add_argument("--fetch-sizes", nargs="+", type=int, default=[1000, 10000, 50000])
    parser.add_argument("--no-materialize", action="store_true", help="Do not build pandas DataFrames in the row path")
    parser.add_argument("--parallel-flight", action="store_true")
    args = parser.parse_args()

    settings = Settings.load()

    print("Query:")
    print(args.query)
    print()

    engine_ms = explain_analyze_time_ms(settings, args.query)
    engine_s = engine_ms / 1000.0
    print(f"PostgreSQL engine time (EXPLAIN ANALYZE): {engine_s:.3f}s")
    print()

    print("Row-oriented PostgreSQL DB-API fetch (JDBC/ODBC-like):")
    for fs in args.fetch_sizes:
        n, dt, _ = direct_pg_fetch(
            settings,
            args.query,
            fetch_size=fs,
            materialize=not args.no_materialize,
        )
        rows_s = n / max(dt, 1e-9)
        transport = max(dt - engine_s, 0.0)
        print(
            f"fetch_size={fs:>6}  rows={n:>10,}  wall={dt:7.2f}s  "
            f"rows/s={rows_s:>12,.0f}  transport≈{transport:6.2f}s"
        )

    print()

    print("Arrow Flight columnar fetch:")
    n, dt, table = flight_fetch(
        args.flight_uri,
        args.query,
        token=args.flight_token,
        ca_path=args.flight_ca,
        parallel=args.parallel_flight,
    )
    rows_s = n / max(dt, 1e-9)
    transport = max(dt - engine_s, 0.0)
    print(
        f"rows={n:>10,}  wall={dt:7.2f}s  rows/s={rows_s:>12,.0f}  "
        f"transport≈{transport:6.2f}s"
    )
    print()
    print("Flight schema:")
    print(table.schema)


if __name__ == "__main__":
    main()