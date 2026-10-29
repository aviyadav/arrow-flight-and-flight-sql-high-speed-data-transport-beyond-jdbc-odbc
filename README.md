# arrow-flight-postgres-demo

A runnable demo of **Apache Arrow Flight** as a columnar transport layer on top of
PostgreSQL. It compares row-oriented (JDBC/ODBC-like) result transfer against
Arrow Flight's columnar, zero-copy IPC streaming, and shows how Flight's control
plane (`GetFlightInfo`, tickets, endpoints, locations) separates query metadata
from bulk data movement.

The goal is to demonstrate one core idea: **query time is often small; transport
time dominates.** Arrow Flight collapses that transport overhead by shipping
Arrow record batches instead of per-cell serialized rows.

---

## What this project demonstrates

| Concept | Where |
| --- | --- |
| Row-major serialization tax vs. columnar wire format | `src/transport/benchmark.py` |
| Engine time vs. transport time decomposition | `EXPLAIN ANALYZE` + benchmark output |
| Arrow IPC columnar streaming | `src/flight/server_minimal.py`, `src/flight/server_sharded.py` |
| `GetFlightInfo` / `FlightInfo` / `Ticket` / `Endpoint` / `Location` | server + client in `src/flight/` |
| `DoGet` (read), `DoPut` (write), `DoAction` (commands) | server + client in `src/flight/` |
| Parallel reads via multiple endpoints | `src/flight/server_sharded.py` + `read_parallel_uri()` |
| Distributed coordinator/worker reads | coordinator + worker runbook flow |
| Prepared statements + parameter binding over Flight | `--prepared-demo` |
| Catalog metadata | `get_catalogs`, `get_db_schemas`, `get_tables` |
| Bearer-token auth middleware | `src/flight/auth.py` |
| TLS transport | `grpc+tls://` + self-signed certs |
| Object storage via S3-compatible RustFS | `src/storage/rustfs.py` |
| Official ADBC Flight SQL client path | `src/flight_sql/adbc_client.py` |

> **Note:** the bundled server uses lightweight **JSON command envelopes** over
> raw Arrow Flight to illustrate Flight SQL concepts (query, catalog, prepared
> statements, transactions). It is *not* the official Flight SQL Protobuf
> protocol. Use `src.flight_sql.adbc_client.py` against a real Flight SQL server
> (Dremio, InfluxDB IOx, Ballista, etc.) for that.

---

## Architecture

```
                       ┌──────────────────────────────────────┐
                       │              PostgreSQL              │
                       │   flight_demo.events                 │
                       │   flight_demo.sales                  │
                       │   flight_demo.flight_upload_demo     │
                       └──────────────┬───────────────────────┘
                                      │ psycopg (row / COPY / server-side cursor)
                                      │
        ┌─────────────────────────────┼─────────────────────────────┐
        │                             │                             │
   [generate_synthetic]         [load_postgres]              [Flight servers]
   CSV + Parquet (+RustFS)      COPY + ANALYZE          minimal  /  sharded  /  coordinator
        │                                                      │  DoGet · DoPut · DoAction
        │                                                      │
        ▼                                                      ▼
   data/csv, data/parquet                              Arrow Flight clients
   data/manifest.json                                   (pyarrow, parallel, ADBC)
```

- **Control plane:** `get_flight_info()` returns metadata (schema, endpoints,
  locations) — small messages over gRPC.
- **Data plane:** `do_get()` streams Arrow `RecordBatch` objects directly.
- **Write plane:** `do_put()` stages Arrow batches in memory; `do_action("commit_ingest")`
  inserts them into PostgreSQL.

---

## Tech stack

- **Python 3.14+** (see `.python-version` and `pyproject.toml`)
- **PyArrow** (Arrow data structures + Flight)
- **psycopg 3** (PostgreSQL connectivity)
- **pandas / polars** (row-path materialization and analysis)
- **boto3** (S3-compatible RustFS access)
- **adbc-driver-flightsql** (official Flight SQL client path)
- **python-dotenv** (configuration)
- **uv** (package/environment management)

---

## Project structure

```
arrow-flight-postgres-demo/
├── certs/                      # self-signed TLS certs (generated on demand)
├── data/                       # generated CSV / Parquet / manifest.json
├── scripts/
│   └── create_db.sql
└── src/
    ├── config.py               # Settings loaded from .env
    ├── arrow_flight_postgres_demo/
    ├── db/
    │   └── postgres.py         # connect, create_schema, COPY, insert, EXPLAIN
    ├── etl/
    │   ├── generate_synthetic.py   # generate CSV + Parquet (+ RustFS upload)
    │   ├── load_postgres.py        # load CSV into PostgreSQL
    │   └── table_defs.py           # Arrow schemas + column lists
    ├── flight/
    │   ├── auth.py                 # bearer-token middleware (server + client)
    │   ├── client.py               # CLI Flight client
    │   ├── common.py               # query streaming, schemas, OID mapping
    │   ├── server_minimal.py       # single-endpoint server (plaintext/TLS)
    │   └── server_sharded.py       # multi-endpoint + coordinator/worker server
    ├── flight_sql/
    │   └── adbc_client.py          # official ADBC Flight SQL client
    ├── storage/
    │   └── rustfs.py               # S3-compatible RustFS helper
    └── transport/
        └── benchmark.py            # row transport vs. Arrow Flight benchmark
```

---

## Prerequisites

- Python 3.14+
- PostgreSQL (15+ recommended) running and reachable
- RustFS (or any S3-compatible endpoint) — required only for `--upload` / object storage
- OpenSSL — required only for TLS

---

## Setup

### 1. Create the database

```bash
createdb flight_demo
# or
psql -U postgres -h localhost -p 5432 -f scripts/create_db.sql
```

### 2. Configure `.env`

Copy `.env.example` to `.env` and fill in your credentials:

```dotenv
PGHOST=localhost
PGPORT=5432
PGDATABASE=flight_demo
PGUSER=postgres
PGPASSWORD=postgres
PGSCHEMA=flight_demo

FLIGHT_HOST=0.0.0.0
FLIGHT_PORT=8815
FLIGHT_TLS=false
FLIGHT_AUTH_TOKEN=

RUSTFS_ENDPOINT=http://localhost:9000
RUSTFS_ACCESS_KEY=rustfsadmin
RUSTFS_SECRET_KEY=rustfsadmin
RUSTFS_BUCKET=arrow-flight-demo
RUSTFS_REGION=us-east-1

DATA_DIR=data
BATCH_SIZE=50000
```

### 3. Install dependencies

```bash
uv sync
```

(Or, without `uv`: `python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`.)

---

## Quick start

```bash
# 1. Generate synthetic data (CSV + Parquet, optionally upload to RustFS)
uv run python -m src.etl.generate_synthetic \
  --events-rows 200000 --sales-rows 100000 --batch-size 50000 --upload

# 2. Load into PostgreSQL
uv run python -m src.etl.load_postgres --manifest data/manifest.json

# 3. Start the Flight server (separate terminal)
uv run python -m src.flight.server_minimal --port 8815 --no-auth

# 4. Read via Arrow Flight
uv run python -m src.flight.client \
  --uri grpc://localhost:8815 \
  --sql "SELECT * FROM events WHERE day='2026-08-01' LIMIT 100"

# 5. Benchmark row vs. columnar transport
uv run python -m src.transport.benchmark \
  --flight-uri grpc://localhost:8815 \
  --query "SELECT * FROM events WHERE day='2026-08-01'" \
  --fetch-sizes 1000 10000 50000
```

For a full, ordered step-by-step walkthrough — including parallel endpoints,
the distributed coordinator/worker pattern, TLS + auth, ingest, prepared
statements, and troubleshooting — see **[RUNBOOK.md](RUNBOOK.md)**.

---

## Feature guide

### Generate synthetic data

```bash
uv run python -m src.etl.generate_synthetic \
  --events-rows 200000 --sales-rows 100000 --batch-size 50000 --upload
```

Produces:

```text
data/csv/events.csv
data/csv/sales.csv
data/parquet/events/part-0000.parquet
data/parquet/sales/part-0000.parquet
data/manifest.json
```

The `events` table has columns `id, day, ts, user_id, kind, payload, amount`;
`sales` has `id, region, amount, ts, day`.

### Load into PostgreSQL

```bash
uv run python -m src.etl.load_postgres --manifest data/manifest.json
```

Creates the `flight_demo` schema and the `events`, `sales`, and
`flight_upload_demo` tables, truncates them, bulk-loads the CSVs with `COPY`,
and runs `ANALYZE`.

### Run the Flight server

Plaintext:

```bash
uv run python -m src.flight.server_minimal --port 8815 --no-auth
# serves grpc://0.0.0.0:8815  →  clients use grpc://localhost:8815
```

TLS + bearer token (requires certs; see RUNBOOK section 10):

```bash
uv run python -m src.flight.server_minimal --port 8815 --tls --token "$FLIGHT_AUTH_TOKEN"
# serves grpc+tls://0.0.0.0:8815
```

### Read data

```bash
# SQL command descriptor
uv run python -m src.flight.client --uri grpc://localhost:8815 --sql "SELECT * FROM events LIMIT 10"

# Named dataset by path (events / sales)
uv run python -m src.flight.client --uri grpc://localhost:8815 --path events

# List available flights
uv run python -m src.flight.client --uri grpc://localhost:8815 --list-flights

# Catalog metadata
uv run python -m src.flight.client --uri grpc://localhost:8815 --metadata get_tables
uv run python -m src.flight.client --uri grpc://localhost:8815 --metadata get_db_schemas
```

### Benchmark row vs. columnar transport

```bash
uv run python -m src.transport.benchmark \
  --flight-uri grpc://localhost:8815 \
  --query "SELECT * FROM events WHERE day='2026-08-01'" \
  --fetch-sizes 1000 10000 50000
```

It reports:

- PostgreSQL engine time (`EXPLAIN ANALYZE`)
- row-oriented DB-API fetch wall-clock time (a JDBC/ODBC stand-in)
- Arrow Flight columnar wall-clock time
- rows/sec for each path

Add `--no-materialize` to skip building pandas DataFrames in the row path.

### Parallel endpoints

```bash
uv run python -m src.flight.server_sharded --port 8815 --shards 4 --no-auth
uv run python -m src.flight.client --uri grpc://localhost:8815 --parallel --sql "SELECT * FROM events WHERE day='2026-08-01'"
```

A single `GetFlightInfo` returns multiple endpoints; the client reads them
concurrently and concatenates the Arrow tables. The sharded server partitions on
`mod(id, N)`, so custom queries must expose an `id` column.

### Distributed coordinator / worker

Start workers on different ports and a coordinator that advertises worker
endpoints, so the client reads bulk data directly from workers while the
coordinator only moves metadata. Full commands are in RUNBOOK section 9.

### Ingest (DoPut) + commit (DoAction)

```bash
uv run python -m src.flight.client \
  --uri grpc://localhost:8815 \
  --put-parquet data/parquet/events/part-0000.parquet \
  --put-path upload_demo --put-rows 10000

uv run python -m src.flight.client --uri grpc://localhost:8815 --list-staging

uv run python -m src.flight.client \
  --uri grpc://localhost:8815 \
  --put-parquet data/parquet/events/part-0000.parquet \
  --put-path upload_demo --put-rows 10000 --commit-table flight_upload_demo
```

Staging is **in-memory** on the server, and `--commit-table` pops the staged
batches and inserts them into the target table (so `--list-staging` is empty
after a successful commit).

### Prepared statements

```bash
uv run python -m src.flight.client --uri grpc://localhost:8815 --prepared-demo
```

Demonstrates create → bind → execute → close using `DoAction` and `DoPut`.

### ADBC Flight SQL client

```bash
export FLIGHT_SQL_URI=grpc+tls://your-flightsql-server:32010
export FLIGHT_SQL_TOKEN="Bearer your-token"
export FLIGHT_SQL_CA=/path/to/ca.pem

uv run python -m src.flight_sql.adbc_client \
  --uri "$FLIGHT_SQL_URI" \
  --sql "SELECT region, sum(amount) AS total FROM sales GROUP BY region" \
  --token "$FLIGHT_SQL_TOKEN" \
  --ca "$FLIGHT_SQL_CA"
```

Use this only against a real Flight SQL server — not the bundled JSON-envelope
server.

---

## Troubleshooting

Common issues (PostgreSQL connection, RustFS upload, Flight connect, auth,
parallel reads, ADBC) are covered in **[RUNBOOK.md §17](RUNBOOK.md)**. A few
quick checks:

- **Flight client can't connect** — make sure the server is running and the URI
  scheme matches: `grpc://` for plaintext, `grpc+tls://` for TLS.
- **TLS with self-signed certs** — pass `--ca certs/ca.pem`.
- **`--list-staging` is empty** — staging is in-memory and consumed on commit
  (or lost on server restart). Put without `--commit-table` to inspect it first.

---

## Production notes

This is a learning/demo system. For production, replace the JSON command
envelopes with official Flight SQL Protobuf, use managed TLS, validate JWT scopes
and row/column policies in middleware, add observability (tracing, rows/sec and
bytes/sec metrics), and tune Arrow batch size to balance memory and latency.
See RUNBOOK §18–§19 for scale-up and production guidance.
