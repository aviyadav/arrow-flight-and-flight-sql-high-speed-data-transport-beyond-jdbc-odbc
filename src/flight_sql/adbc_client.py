from __future__ import annotations

import argparse
import json
import os

import adbc_driver_flightsql.dbapi as flight_sql


def connect(
    uri: str,
    token: str | None = None,
    ca_path: str | None = None,
):
    db_kwargs = {}

    if token:
        db_kwargs["adbc.flight.sql.authorization_header"] = f"Bearer {token}"

    if ca_path:
        db_kwargs["adbc.flight.sql.client_option.tls_root_certs"] = ca_path

    return flight_sql.connect(uri, db_kwargs=db_kwargs)


def run_query(
    uri: str,
    sql_text: str,
    params=None,
    token: str | None = None,
    ca_path: str | None = None,
):
    conn = connect(uri, token=token, ca_path=ca_path)
    cur = conn.cursor()

    if params:
        cur.execute(sql_text, parameters=tuple(params))
    else:
        cur.execute(sql_text)

    table = cur.fetch_arrow_table()

    cur.close()
    conn.close()
    return table


def main() -> None:
    parser = argparse.ArgumentParser(
        description="""
        ADBC Flight SQL client.

        This requires a real Flight SQL server, for example:
        - Dremio Flight SQL endpoint
        - InfluxDB 3.x Flight SQL endpoint
        - Ballista Flight SQL endpoint
        - an ADBC Flight SQL server

        It does NOT connect to the raw Flight server in this repo.
        """
    )
    parser.add_argument("--uri", default=os.getenv("FLIGHT_SQL_URI"))
    parser.add_argument("--sql", required=True)
    parser.add_argument("--params", default=None, help="JSON array of bind parameters")
    parser.add_argument("--token", default=os.getenv("FLIGHT_SQL_TOKEN"))
    parser.add_argument("--ca", default=os.getenv("FLIGHT_SQL_CA"))
    args = parser.parse_args()

    if not args.uri:
        raise SystemExit("Set --uri or FLIGHT_SQL_URI to a real Flight SQL endpoint")

    params = json.loads(args.params) if args.params else None

    table = run_query(
        args.uri,
        args.sql,
        params=params,
        token=args.token,
        ca_path=args.ca,
    )

    print("Rows:", table.num_rows)
    print(table.schema)
    print(table.slice(0, min(10, table.num_rows)).to_pydict())


if __name__ == "__main__":
    main()