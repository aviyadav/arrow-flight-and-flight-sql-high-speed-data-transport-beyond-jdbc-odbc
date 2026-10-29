# Below is a complete, runnable project skeleton that covers the article’s concepts end-to-end using:

* PostgreSQL as the analytical/query database  
* Python + PyArrow Flight as the high-speed columnar transport  
* A Flight-SQL-style command layer over raw Flight for runnable demos  
* ADBC Flight SQL client templates for real Flight SQL servers  
* RustFS as the S3-compatible object store for generated Parquet files  
* Synthetic data generation + loading programs  
* Benchmarks showing engine time vs transport time  
* Parallel endpoints and distributed coordinator/worker read fan-out  
* TLS + bearer-token auth middleware  
* DoGet / DoPut / DoAction / prepared-statement style demos

Important note about “Flight SQL”:

* Official Flight SQL is a Protobuf-defined protocol layered on Arrow Flight.  
* A fully compliant Flight SQL server is non-trivial and usually comes from engines like Dremio, InfluxDB 3.x, Ballista, or an ADBC Flight SQL server.  
* This project gives you:  
  1. A runnable Arrow Flight service over PostgreSQL.  
  2. A runnable “Flight-SQL-style” command layer using JSON command envelopes over the same Flight verbs: query, catalog metadata, prepared statements, transactions.  
  3. Real ADBC Flight SQL client code that you can point at an actual Flight SQL server.

So the project is fully runnable against PostgreSQL + RustFS, while also showing the official ADBC/Flight SQL client path.  
---

# 1. Project layout

Create this directory structure:  
```bash  
mkdir -p arrow-flight-postgres-demo  
cd arrow-flight-postgres-demo

mkdir -p src/db src/etl src/flight src/flight_sql src/storage src/transport  
mkdir -p certs data/csv data/parquet scripts

touch src/__init__.py  
touch src/db/__init__.py  
touch src/etl/__init__.py  
touch src/flight/__init__.py  
touch src/flight_sql/__init__.py  
touch src/storage/__init__.py  
touch src/transport/__init__.py  
```
Final tree:  
```text  
arrow-flight-postgres-demo/  
├── .env.example  
├── requirements.txt  
├── README_RUNBOOK.md  
├── certs/  
├── data/  
│   ├── csv/  
│   └── parquet/  
├── scripts/  
│   └── create_db.sql  
└── src/  
    ├── __init__.py  
    ├── config.py  
    ├── db/  
    │   ├── __init__.py  
    │   └── postgres.py  
    ├── etl/  
    │   ├── __init__.py  
    │   ├── table_defs.py  
    │   ├── generate_synthetic.py  
    │   └── load_postgres.py  
    ├── flight/  
    │   ├── __init__.py  
    │   ├── auth.py  
    │   ├── common.py  
    │   ├── server_minimal.py  
    │   ├── server_sharded.py  
    │   └── client.py  
    ├── flight_sql/  
    │   ├── __init__.py  
    │   └── adbc_client.py  
    ├── storage/  
    │   ├── __init__.py  
    │   └── rustfs.py  
    └── transport/  
        ├── __init__.py  
        └── benchmark.py  
```

# 2. Requirements

requirements.txt  
```text  
pyarrow>=17.0.0  
psycopg[binary]>=3.2.0  
python-dotenv>=1.0.0  
boto3>=1.34.0  
pandas>=2.2.0  
adbc-driver-manager>=1.0.0  
adbc-driver-flightsql>=1.0.0  
```

# 3. Environment file

.env.example  
```dotenv  
# PostgreSQL  
PGHOST=localhost  
PGPORT=5432  
PGDATABASE=flight_demo  
PGUSER=postgres  
PGPASSWORD=postgres  
PGSCHEMA=flight_demo

# Arrow Flight server  
FLIGHT_HOST=0.0.0.0  
FLIGHT_PORT=8815  
FLIGHT_TLS=false  
FLIGHT_CERT_PATH=certs/server.crt  
FLIGHT_KEY_PATH=certs/server.key  
FLIGHT_CA_PATH=certs/ca.pem  
FLIGHT_AUTH_TOKEN=

# RustFS / S3-compatible object storage  
RUSTFS_ENDPOINT=http\://localhost:9000  
RUSTFS_ACCESS_KEY=rustfsadmin  
RUSTFS_SECRET_KEY=rustfsadmin  
RUSTFS_BUCKET=arrow-flight-demo  
RUSTFS_REGION=us-east-1

# Data generation  
DATA_DIR=data  
BATCH_SIZE=50000  
```
Copy it:  
```bash  
cp .env.example .env  
```
Edit .env to match your PostgreSQL and RustFS settings.  
---

# 4. Source code

## 4.1 src/config.py

```python  
from __future__ import annotations

import os  
from dataclasses import dataclass, replace

from dotenv import load_dotenv

load_dotenv()

def _bool(name: str, default: bool = False) -> bool:  
    val = os.getenv(name)  
    if val is None:  
        return default  
    return val.strip().lower() in {"1", "true", "yes", "y", "on"}

@dataclass(frozen=True)  
class Settings:  
    pg_host: str  
    pg_port: int  
    pg_db: str  
    pg_user: str  
    pg_password: str  
    pg_schema: str

    flight_host: str  
    flight_port: int  
    flight_tls: bool  
    flight_cert: str  
    flight_key: str  
    flight_ca: str  
    flight_auth_token: str

    rustfs_endpoint: str  
    rustfs_access_key: str  
    rustfs_secret_key: str  
    rustfs_bucket: str  
    rustfs_region: str

    data_dir: str  
    batch_size: int

    @classmethod  
    def load(cls) -> "Settings":  
        return cls(  
            pg_host=os.getenv("PGHOST", "localhost"),  
            pg_port=int(os.getenv("PGPORT", "5432")),  
            pg_db=os.getenv("PGDATABASE", "flight_demo"),  
            pg_user=os.getenv("PGUSER", "postgres"),  
            pg_password=os.getenv("PGPASSWORD", ""),  
            pg_schema=os.getenv("PGSCHEMA", "flight_demo"),  
            flight_host=os.getenv("FLIGHT_HOST", "0.0.0.0"),  
            flight_port=int(os.getenv("FLIGHT_PORT", "8815")),  
            flight_tls=_bool("FLIGHT_TLS", False),  
            flight_cert=os.getenv("FLIGHT_CERT_PATH", "certs/server.crt"),  
            flight_key=os.getenv("FLIGHT_KEY_PATH", "certs/server.key"),  
            flight_ca=os.getenv("FLIGHT_CA_PATH", "certs/ca.pem"),  
            flight_auth_token=os.getenv("FLIGHT_AUTH_TOKEN", ""),  
            rustfs_endpoint=os.getenv("RUSTFS_ENDPOINT", ""),  
            rustfs_access_key=os.getenv("RUSTFS_ACCESS_KEY", ""),  
            rustfs_secret_key=os.getenv("RUSTFS_SECRET_KEY", ""),  
            rustfs_bucket=os.getenv("RUSTFS_BUCKET", "arrow-flight-demo"),  
            rustfs_region=os.getenv("RUSTFS_REGION", "us-east-1"),  
            data_dir=os.getenv("DATA_DIR", "data"),  
            batch_size=int(os.getenv("BATCH_SIZE", "50000")),  
        )

    @property  
    def pg_dsn(self) -> str:  
        return (  
            f"host={self.pg_host} "  
            f"port={self.pg_port} "  
            f"dbname={self.pg_db} "  
            f"user={self.pg_user} "  
            f"password={self.pg_password} "  
            f"connect_timeout=10"  
        )

    @property  
    def flight_scheme(self) -> str:  
        return "grpc+tls" if self.flight_tls else "grpc"

    @property  
    def flight_listen_location(self) -> str:  
        return f"{self.flight_scheme}://{self.flight_host}:{self.flight_port}"

    @property  
    def flight_client_location(self) -> str:  
        host = "localhost" if self.flight_host in ("0.0.0.0", "::") else self.flight_host  
        return f"{self.flight_scheme}://{host}:{self.flight_port}"

    def replace(self, **kwargs):  
        return replace(self, **kwargs)  
```

## 4.2 src/db/postgres.py

```python  
from __future__ import annotations

from typing import Sequence

import psycopg  
from psycopg import sql  
from psycopg.rows import tuple_row

from src.config import Settings

def connect(  
    settings: Settings,  
    autocommit: bool = False,  
    row_factory=tuple_row,  
):  
    conn = psycopg.connect(  
        settings.pg_dsn,  
        autocommit=autocommit,  
        row_factory=row_factory,  
    )  
    with conn.cursor() as cur:  
        cur.execute("SET search_path TO %s, public", (settings.pg_schema,))  
    if not autocommit:  
        conn.commit()  
    return conn

def create_schema(settings: Settings) -> None:  
    conn = connect(settings, autocommit=True)  
    schema = sql.Identifier(settings.pg_schema)

    conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(schema))

    conn.execute(  
        sql.SQL(  
            """  
            CREATE TABLE IF NOT EXISTS {schema}.events (  
                id BIGINT PRIMARY KEY,  
                day DATE NOT NULL,  
                ts TIMESTAMP NOT NULL,  
                user_id INTEGER NOT NULL,  
                kind TEXT NOT NULL,  
                payload TEXT,  
                amount DOUBLE PRECISION NOT NULL  
            )  
            """  
        ).format(schema=schema)  
    )

    conn.execute(  
        sql.SQL(  
            """  
            CREATE TABLE IF NOT EXISTS {schema}.sales (  
                id BIGINT PRIMARY KEY,  
                region TEXT NOT NULL,  
                amount DOUBLE PRECISION NOT NULL,  
                ts TIMESTAMP NOT NULL,  
                day DATE NOT NULL  
            )  
            """  
        ).format(schema=schema)  
    )

    conn.execute(  
        sql.SQL(  
            """  
            CREATE TABLE IF NOT EXISTS {schema}.flight_upload_demo (  
                id BIGINT,  
                day DATE,  
                ts TIMESTAMP,  
                user_id INTEGER,  
                kind TEXT,  
                payload TEXT,  
                amount DOUBLE PRECISION  
            )  
            """  
        ).format(schema=schema)  
    )

    idx_events_day = sql.Identifier(f"{settings.pg_schema}_events_day_idx")  
    idx_events_day_kind = sql.Identifier(f"{settings.pg_schema}_events_day_kind_idx")  
    idx_sales_day = sql.Identifier(f"{settings.pg_schema}_sales_day_idx")

    conn.execute(  
        sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {}.events (day)").format(  
            idx_events_day, schema  
        )  
    )  
    conn.execute(  
        sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {}.events (day, kind)").format(  
            idx_events_day_kind, schema  
        )  
    )  
    conn.execute(  
        sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {}.sales (day)").format(  
            idx_sales_day, schema  
        )  
    )

    conn.close()

def truncate_tables(settings: Settings) -> None:  
    conn = connect(settings, autocommit=True)  
    schema = sql.Identifier(settings.pg_schema)  
    for table in ("events", "sales", "flight_upload_demo"):  
        conn.execute(  
            sql.SQL("TRUNCATE TABLE {}.{} RESTART IDENTITY CASCADE").format(  
                schema, sql.Identifier(table)  
            )  
        )  
    conn.close()

def copy_csv_to_table(  
    settings: Settings,  
    table: str,  
    csv_path: str,  
    columns: Sequence[str],  
) -> None:  
    conn = connect(settings)  
    schema = sql.Identifier(settings.pg_schema)  
    table_id = sql.Identifier(table)  
    cols_sql = sql.SQL(", ").join(sql.Identifier(c) for c in columns)

    query = sql.SQL("COPY {schema}.{table} ({cols}) FROM STDIN WITH (FORMAT csv)").format(  
        schema=schema,  
        table=table_id,  
        cols=cols_sql,  
    )

    with conn.cursor() as cur:  
        with cur.copy(query) as copy:  
            with open(csv_path, "rb") as f:  
                while True:  
                    chunk = f.read(1024 * 1024\)  
                    if not chunk:  
                        break  
                    copy.write(chunk)

    conn.commit()  
    conn.close()

def insert_record_batches(settings: Settings, table: str, batches) -> int:  
    if not batches:  
        return 0

    conn = connect(settings)  
    schema = sql.Identifier(settings.pg_schema)  
    table_id = sql.Identifier(table)

    first = batches[0]  
    cols = first.schema.names  
    col_ids = sql.SQL(", ").join(sql.Identifier(c) for c in cols)  
    placeholders = sql.SQL(", ").join([sql.Placeholder()] * len(cols))

    stmt = sql.SQL("INSERT INTO {schema}.{table} ({cols}) VALUES ({vals})").format(  
        schema=schema,  
        table=table_id,  
        cols=col_ids,  
        vals=placeholders,  
    )

    inserted = 0  
    with conn.cursor() as cur:  
        for batch in batches:  
            rows = batch.to_pylist()  
            values = [tuple(row[c] for c in cols) for row in rows]  
            if values:  
                cur.executemany(stmt, values)  
                inserted += len(values)

    conn.commit()  
    conn.close()  
    return inserted

def explain_analyze_time_ms(settings: Settings, query: str) -> float:  
    conn = connect(settings)  
    with conn.cursor() as cur:  
        cur.execute(  
            sql.SQL("EXPLAIN (ANALYZE, FORMAT JSON) {query}").format(  
                query=sql.SQL(query)  
            )  
        )  
        result = cur.fetchone()[0]  
    conn.commit()  
    conn.close()  
    return float(result[0]["Execution Time"])  
```

## 4.3 src/storage/rustfs.py

```python  
from __future__ import annotations

import argparse  
from pathlib import Path

import boto3  
from botocore.config import Config  
from botocore.exceptions import ClientError

from src.config import Settings

def s3_resource(settings: Settings):  
    if not settings.rustfs_endpoint:  
        return None

    config = Config(  
        signature_version="s3v4",  
        s3={"addressing_style": "path"},  
    )

    return boto3.resource(  
        "s3",  
        endpoint_url=settings.rustfs_endpoint,  
        aws_access_key_id=settings.rustfs_access_key,  
        aws_secret_access_key=settings.rustfs_secret_key,  
        region_name=settings.rustfs_region,  
        config=config,  
    )

def ensure_bucket(settings: Settings):  
    s3 = s3_resource(settings)  
    if s3 is None:  
        return None

    try:  
        s3.meta.client.head_bucket(Bucket=settings.rustfs_bucket)  
    except ClientError as e:  
        error_code = int(e.response.get("Error", {}).get("Code", 0))  
        if error_code == 404:  
            s3.create_bucket(Bucket=settings.rustfs_bucket)  
        else:  
            raise

    return s3

def upload_file(settings: Settings, local_path: Path \| str, key: str) -> str \| None:  
    if not settings.rustfs_endpoint:  
        return None

    s3 = ensure_bucket(settings)  
    if s3 is None:  
        return None

    local_path = Path(local_path)  
    s3.Object(settings.rustfs_bucket, key).upload_file(str(local_path))  
    return f"s3://{settings.rustfs_bucket}/{key}"

def list_objects(settings: Settings, prefix: str = ""):  
    s3 = s3_resource(settings)  
    if s3 is None:  
        return []

    bucket = s3.Bucket(settings.rustfs_bucket)  
    return bucket.objects.filter(Prefix=prefix)

def main() -> None:  
    parser = argparse.ArgumentParser(description="RustFS / S3 utility")  
    parser.add_argument("--list", action="store_true", help="List objects")  
    parser.add_argument("--prefix", default="", help="Key prefix")  
    parser.add_argument("--upload", action="store_true", help="Upload a file")  
    parser.add_argument("--file", help="Local file to upload")  
    parser.add_argument("--key", help="Remote object key")  
    args = parser.parse_args()

    settings = Settings.load()

    if args.upload:  
        if not args.file or not args.key:  
            raise SystemExit("--upload requires --file and --key")  
        uri = upload_file(settings, args.file, args.key)  
        print(f"Uploaded to {uri}")

    if args.list:  
        for obj in list_objects(settings, args.prefix):  
            print(f"{obj.key}  {obj.size} bytes")

if __name__ == "__main__":  
    main()  
```

## 4.4 src/etl/table_defs.py

```python  
import pyarrow as pa

EVENT_COLUMNS = [  
    "id",  
    "day",  
    "ts",  
    "user_id",  
    "kind",  
    "payload",  
    "amount",  
]

SALES_COLUMNS = [  
    "id",  
    "region",  
    "amount",  
    "ts",  
    "day",  
]

EVENT_SCHEMA = pa.schema(  
    [  
        pa.field("id", pa.int64(), nullable=False),  
        pa.field("day", pa.date32(), nullable=False),  
        pa.field("ts", pa.timestamp("us"), nullable=False),  
        pa.field("user_id", pa.int32(), nullable=False),  
        pa.field("kind", pa.string(), nullable=False),  
        pa.field("payload", pa.string(), nullable=True),  
        pa.field("amount", pa.float64(), nullable=False),  
    ]  
)

SALES_SCHEMA = pa.schema(  
    [  
        pa.field("id", pa.int64(), nullable=False),  
        pa.field("region", pa.string(), nullable=False),  
        pa.field("amount", pa.float64(), nullable=False),  
        pa.field("ts", pa.timestamp("us"), nullable=False),  
        pa.field("day", pa.date32(), nullable=False),  
    ]  
)  
```

## 4.5 src/etl/generate_synthetic.py

```python  
from __future__ import annotations

import argparse  
import csv  
import json  
import random  
from datetime import date, datetime, time, timedelta, timezone  
from pathlib import Path

import pyarrow as pa  
import pyarrow.parquet as pq

from src.config import Settings  
from src.etl.table_defs import EVENT_SCHEMA, SALES_SCHEMA  
from src.storage.rustfs import ensure_bucket, upload_file

TARGET_DAY = date(2026, 8, 1\)  
START_DAY = date(2026, 7, 1\)  
END_DAY = date(2026, 8, 31\)  
NUM_DAYS = (END_DAY - START_DAY).days + 1

KINDS = ["click", "view", "purchase", "refund", "signup"]  
REGIONS = ["west", "east", "north", "south"]  
DEVICES = ["mobile", "web", "api"]

def random_day(rng: random.Random) -> date:  
    if rng.random() < 0.5:  
        return TARGET_DAY  
    return START_DAY + timedelta(days=rng.randrange(NUM_DAYS))

def random_ts(rng: random.Random, day: date) -> datetime:  
    return datetime.combine(day, time(0, 0)) + timedelta(seconds=rng.randrange(86400))

def make_event_rows(start_id: int, n_rows: int, rng: random.Random):  
    for offset in range(n_rows):  
        i = start_id + offset  
        day = random_day(rng)  
        ts = random_ts(rng, day)  
        user_id = rng.randrange(1, 1_000_000)  
        kind = rng.choice(KINDS)  
        payload = json.dumps(  
            {  
                "event_id": i,  
                "kind": kind,  
                "device": rng.choice(DEVICES),  
                "session": rng.randrange(1, 100_000),  
            }  
        )  
        amount = round(rng.uniform(1.0, 500.0), 2\) if kind in {"purchase", "refund"} else 0.0  
        yield (i, day, ts, user_id, kind, payload, amount)

def make_sales_rows(start_id: int, n_rows: int, rng: random.Random):  
    for offset in range(n_rows):  
        i = start_id + offset  
        day = random_day(rng)  
        ts = random_ts(rng, day)  
        region = rng.choice(REGIONS)  
        amount = round(rng.uniform(10.0, 10_000.0), 2\)  
        yield (i, region, amount, ts, day)

def csv_value(value) -> str:  
    if value is None:  
        return ""  
    if isinstance(value, bool):  
        return "true" if value else "false"  
    if isinstance(value, datetime):  
        return value.isoformat(sep=" ")  
    if isinstance(value, date):  
        return value.isoformat()  
    return str(value)

def write_batch(  
    csv_writer: csv.writer,  
    parquet_writer: pq.ParquetWriter,  
    rows: list[tuple],  
    schema: pa.Schema,  
) -> None:  
    for row in rows:  
        csv_writer.writerow([csv_value(v) for v in row])

    cols = list(zip(*rows))  
    arrays = [  
        pa.array(list(col), type=schema.field(i).type)  
        for i, col in enumerate(cols)  
    ]  
    rb = pa.RecordBatch.from_arrays(arrays, schema=schema)  
    table = pa.Table.from_batches([rb], schema=schema)  
    parquet_writer.write_table(table)

def generate_and_write(  
    table_name: str,  
    row_iter,  
    csv_path: Path,  
    parquet_path: Path,  
    schema: pa.Schema,  
    settings: Settings,  
    batch_size: int,  
    upload: bool,  
) -> dict:  
    csv_path.parent.mkdir(parents=True, exist_ok=True)  
    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    parquet_writer = pq.ParquetWriter(str(parquet_path), schema)  
    count = 0

    with open(csv_path, "w", newline="") as f:  
        writer = csv.writer(f, lineterminator="\\n")  
        batch = []

        for row in row_iter:  
            batch.append(row)  
            count += 1

            if len(batch) >= batch_size:  
                write_batch(writer, parquet_writer, batch, schema)  
                batch.clear()

        if batch:  
            write_batch(writer, parquet_writer, batch, schema)

    parquet_writer.close()

    rustfs_key = None  
    if upload and settings.rustfs_endpoint:  
        ensure_bucket(settings)  
        rustfs_key = f"demo/{table_name}/part-0000.parquet"  
        upload_file(settings, parquet_path, rustfs_key)  
    elif upload and not settings.rustfs_endpoint:  
        print(f"WARNING: --upload requested but RUSTFS_ENDPOINT is empty; skipping upload for {table_name}")

    return {  
        "rows": count,  
        "csv": str(csv_path),  
        "parquet": str(parquet_path),  
        "rustfs_key": rustfs_key,  
    }

def main() -> None:  
    parser = argparse.ArgumentParser(description="Generate synthetic PostgreSQL demo data")  
    parser.add_argument("--events-rows", type=int, default=200_000)  
    parser.add_argument("--sales-rows", type=int, default=100_000)  
    parser.add_argument("--batch-size", type=int, default=50_000)  
    parser.add_argument("--seed", type=int, default=42)  
    parser.add_argument("--out-dir", default=None)  
    parser.add_argument("--upload", action="store_true", help="Upload Parquet files to RustFS")  
    args = parser.parse_args()

    settings = Settings.load()  
    out_dir = Path(args.out_dir or settings.data_dir)

    csv_dir = out_dir / "csv"  
    parquet_dir = out_dir / "parquet"

    rng = random.Random(args.seed)

    manifest = {  
        "generated_at": datetime.now(timezone.utc).isoformat(),  
        "target_day": TARGET_DAY.isoformat(),  
        "tables": {},  
    }

    manifest["tables"]["events"] = generate_and_write(  
        table_name="events",  
        row_iter=make_event_rows(1, args.events_rows, rng),  
        csv_path=csv_dir / "events.csv",  
        parquet_path=parquet_dir / "events" / "part-0000.parquet",  
        schema=EVENT_SCHEMA,  
        settings=settings,  
        batch_size=args.batch_size,  
        upload=args.upload,  
    )

    manifest["tables"]["sales"] = generate_and_write(  
        table_name="sales",  
        row_iter=make_sales_rows(1, args.sales_rows, rng),  
        csv_path=csv_dir / "sales.csv",  
        parquet_path=parquet_dir / "sales" / "part-0000.parquet",  
        schema=SALES_SCHEMA,  
        settings=settings,  
        batch_size=args.batch_size,  
        upload=args.upload,  
    )

    manifest_path = out_dir / "manifest.json"  
    manifest_path.parent.mkdir(parents=True, exist_ok=True)  
    with open(manifest_path, "w") as f:  
        json.dump(manifest, f, indent=2)

    print(f"Wrote manifest: {manifest_path}")  
    print(json.dumps(manifest, indent=2))

if __name__ == "__main__":  
    main()  
```
---

## 4.6 src/etl/load_postgres.py

```python  
from __future__ import annotations

import argparse  
import json  
from pathlib import Path

from src.config import Settings  
from src.db.postgres import (  
    connect,  
    copy_csv_to_table,  
    create_schema,  
    truncate_tables,  
)  
from src.etl.table_defs import EVENT_COLUMNS, SALES_COLUMNS

def main() -> None:  
    parser = argparse.ArgumentParser(description="Load synthetic CSV data into PostgreSQL")  
    parser.add_argument("--manifest", default="data/manifest.json")  
    args = parser.parse_args()

    settings = Settings.load()  
    manifest_path = Path(args.manifest)

    if not manifest_path.exists():  
        raise SystemExit(f"Manifest not found: {manifest_path}. Run generate_synthetic first.")

    with open(manifest_path) as f:  
        manifest = json.load(f)

    create_schema(settings)  
    truncate_tables(settings)

    for table_name, columns in (  
        ("events", EVENT_COLUMNS),  
        ("sales", SALES_COLUMNS),  
    ):  
        meta = manifest["tables"][table_name]  
        csv_path = meta["csv"]  
        print(f"Loading {table_name} from {csv_path}")  
        copy_csv_to_table(settings, table_name, csv_path, columns)

    conn = connect(settings, autocommit=True)  
    conn.execute("ANALYZE events")  
    conn.execute("ANALYZE sales")  
    conn.close()

    print("Load complete.")

if __name__ == "__main__":  
    main()  
```

## 4.7 src/flight/common.py

```python  
from __future__ import annotations

from datetime import date, datetime, timezone  
from decimal import Decimal  
from typing import Any

import pyarrow as pa  
import pyarrow.flight as flight

from src.config import Settings  
from src.db.postgres import connect

PG_OID_TO_ARROW: dict[int, pa.DataType] = {  
    16: pa.bool_(),  
    20: pa.int64(),  
    21: pa.int64(),  
    23: pa.int64(),  
    26: pa.int64(),  
    700: pa.float64(),  
    701: pa.float64(),  
    1700: pa.float64(),  
    1082: pa.date32(),  
    1114: pa.timestamp("us"),  
    1184: pa.timestamp("us", tz="UTC"),  
    17: pa.binary(),  
    25: pa.string(),  
    1042: pa.string(),  
    1043: pa.string(),  
    19: pa.string(),  
    114: pa.string(),  
    3802: pa.string(),  
}

def make_location(uri: str):  
    try:  
        return flight.Location.parse(uri)  
    except Exception:  
        if uri.startswith("grpc+tls://"):  
            rest = uri.split("://", 1)[1]  
            host, port = rest.rsplit(":", 1\)  
            return flight.Location.for_grpc_tls(host, int(port))  
        if uri.startswith("grpc://"):  
            rest = uri.split("://", 1)[1]  
            host, port = rest.rsplit(":", 1\)  
            return flight.Location.for_grpc_tcp(host, int(port))  
        return uri

def pg_oid_to_arrow(oid: int \| None) -> pa.DataType:  
    if oid is None:  
        return pa.string()  
    return PG_OID_TO_ARROW.get(oid, pa.string())

def schema_from_description(description) -> pa.Schema:  
    if description is None:  
        return pa.schema([])

    fields = []  
    for col in description:  
        name = getattr(col, "name", None)  
        if name is None and isinstance(col, tuple):  
            name = col[0]

        oid = getattr(col, "type_code", None)  
        if oid is None and isinstance(col, tuple) and len(col) > 1:  
            oid = col[1]

        fields.append(pa.field(str(name), pg_oid_to_arrow(oid), nullable=True))

    return pa.schema(fields)

def convert_value(value: Any, arrow_type: pa.DataType) -> Any:  
    if value is None:  
        return None

    if isinstance(value, Decimal):  
        if pa.types.is_float64(arrow_type) or pa.types.is_float32(arrow_type):  
            return float(value)  
        if pa.types.is_decimal(arrow_type):  
            return value  
        return str(value)

    if isinstance(value, datetime):  
        if pa.types.is_timestamp(arrow_type):  
            if arrow_type.tz is None:  
                if value.tzinfo is not None:  
                    value = value.astimezone(timezone.utc).replace(tzinfo=None)  
            else:  
                if value.tzinfo is None:  
                    value = value.replace(tzinfo=timezone.utc)  
                else:  
                    value = value.astimezone(timezone.utc)  
            return value

    if isinstance(value, date) and not isinstance(value, datetime):  
        if pa.types.is_date32(arrow_type) or pa.types.is_date64(arrow_type):  
            return value

    if isinstance(value, bytes):  
        if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):  
            return value.decode("utf-8", errors="replace")  
        return value

    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):  
        if not isinstance(value, str):  
            return str(value)

    return value

def rows_to_batch(rows: list[tuple], schema: pa.Schema) -> pa.RecordBatch:  
    arrays = []  
    for i, field in enumerate(schema):  
        values = [convert_value(row[i], field.type) for row in rows]  
        arrays.append(pa.array(values, type=field.type))  
    return pa.RecordBatch.from_arrays(arrays, schema=schema)

def make_limit0_sql(sql_text: str) -> str:  
    q = sql_text.strip().rstrip(";")  
    lower = q.lower()

    if lower.startswith("select"):  
        return f"SELECT * FROM ({q}) AS _flight_limit0 LIMIT 0"

    if lower.startswith("with"):  
        return f"{q} LIMIT 0"

    return f"{q} LIMIT 0"

def _execute(cur, stmt: str, params=None):  
    if params:  
        cur.execute(stmt, params)  
    else:  
        cur.execute(stmt)

def get_result_schema(  
    settings: Settings,  
    sql_text: str,  
    params=None,  
) -> pa.Schema:  
    limited = make_limit0_sql(sql_text)

    conn = connect(settings)  
    with conn.cursor() as cur:  
        _execute(cur, limited, params)  
        schema = schema_from_description(cur.description)  
    conn.commit()  
    conn.close()  
    return schema

def make_query_stream(  
    settings: Settings,  
    sql_text: str,  
    batch_size: int = 10_000,  
    params=None,  
):  
    conn = connect(settings)  
    cur = conn.cursor(name="flight_stream")  
    cur.itersize = batch_size

    _execute(cur, sql_text, params)  
    schema = schema_from_description(cur.description)

    def gen():  
        try:  
            while True:  
                rows = cur.fetchmany(batch_size)  
                if not rows:  
                    break  
                yield rows_to_batch(rows, schema)  
        finally:  
            try:  
                cur.close()  
            finally:  
                conn.close()

    return schema, gen()  
```

## 4.8 src/flight/auth.py

```python  
from __future__ import annotations

import pyarrow.flight as flight

def _decode_maybe(value):  
    if isinstance(value, bytes):  
        return value.decode("utf-8", errors="replace")  
    return value

def _first_value(value):  
    if isinstance(value, list):  
        value = value[0] if value else None  
    return _decode_maybe(value)

def _get_header(headers, name: str):  
    if headers is None:  
        return None

    for key in (name, name.encode()):  
        try:  
            if key in headers:  
                return _first_value(headers[key])  
        except TypeError:  
            pass

    try:  
        items = headers.items()  
    except AttributeError:  
        items = []

    for k, v in items:  
        ks = _decode_maybe(k)  
        if isinstance(ks, str) and ks.lower() == name.lower():  
            return _first_value(v)

    return None

def _extract_bearer(raw):  
    raw = _decode_maybe(raw)  
    if not raw:  
        return None  
    parts = raw.split()  
    if len(parts) == 2 and parts[0].lower() == "bearer":  
        return parts[1]  
    return raw

class BearerAuthFactory(flight.ServerMiddlewareFactory):  
    def __init__(self, expected_token: str):  
        self.expected_token = expected_token

    def start_call(self, info, headers=None):  
        hdrs = headers if headers is not None else getattr(info, "headers", None)  
        raw = _get_header(hdrs, "authorization")  
        token = _extract_bearer(raw)

        if token \!= self.expected_token:  
            raise flight.FlightUnauthenticatedError("invalid or missing bearer token")

        return ServerAuthMiddleware(token)

class ServerAuthMiddleware(flight.ServerMiddleware):  
    def __init__(self, token: str):  
        self.token = token

class TokenClientMiddlewareFactory(flight.ClientMiddlewareFactory):  
    def __init__(self, token: str):  
        self.token = token

    def start_call(self, info):  
        return TokenClientMiddleware(self.token)

class TokenClientMiddleware(flight.ClientMiddleware):  
    def __init__(self, token: str):  
        self.token = token

    def sending_headers(self):  
        return {"authorization": f"Bearer {self.token}"}  
```

## 4.9 src/flight/server_minimal.py

```python  
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
        location: str \| None = None,  
        advertised_location: str \| None = None,  
        batch_size: int \| None = None,  
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
                    batches = [batch for batch in reader]  
                    params = self._batch_to_params(batches[0]) if batches else ()  
                    with self._lock:  
                        self._prepared_params[handle] = params  
                    writer.write(b"bound")  
                    return

        name = descriptor.path[0].decode() if descriptor.path else "default"  
        batches = [batch for batch in reader]

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
```

## 4.10 src/flight/server_sharded.py

```python  
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
        worker_uris: list[str] \| None = None,  
        location: str \| None = None,  
        advertised_location: str \| None = None,  
        batch_size: int \| None = None,  
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
```

## 4.11 src/flight/client.py

```python  
from __future__ import annotations

import argparse  
import json  
from concurrent.futures import ThreadPoolExecutor  
from datetime import date

import pyarrow as pa  
import pyarrow.flight as flight  
import pyarrow.parquet as pq

from src.flight.auth import TokenClientMiddlewareFactory

def connect_client(uri: str, token: str \| None = None, ca_path: str \| None = None):  
    middleware = [TokenClientMiddlewareFactory(token)] if token else None  
    tls_root_certs = None

    if ca_path:  
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

def read_sql(uri: str, sql_text: str, token: str \| None = None, ca_path: str \| None = None) -> pa.Table:  
    client = connect_client(uri, token=token, ca_path=ca_path)  
    descriptor = flight.FlightDescriptor.for_command(sql_text.encode())  
    info = client.get_flight_info(descriptor)  
    return client.do_get(info.endpoints[0].ticket).read_all()

def read_path(uri: str, path: str, token: str \| None = None, ca_path: str \| None = None) -> pa.Table:  
    client = connect_client(uri, token=token, ca_path=ca_path)  
    descriptor = flight.FlightDescriptor.for_path(path)  
    info = client.get_flight_info(descriptor)  
    return client.do_get(info.endpoints[0].ticket).read_all()

def read_metadata(uri: str, kind: str, token: str \| None = None, ca_path: str \| None = None) -> pa.Table:  
    client = connect_client(uri, token=token, ca_path=ca_path)  
    cmd = json.dumps({"type": kind}).encode()  
    descriptor = flight.FlightDescriptor.for_command(cmd)  
    info = client.get_flight_info(descriptor)  
    return client.do_get(info.endpoints[0].ticket).read_all()

def read_parallel_uri(  
    uri: str,  
    sql_text: str,  
    token: str \| None = None,  
    ca_path: str \| None = None,  
    max_workers: int \| None = None,  
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

def put_table(uri: str, path: str, table: pa.Table, token: str \| None = None, ca_path: str \| None = None):  
    client = connect_client(uri, token=token, ca_path=ca_path)  
    descriptor = flight.FlightDescriptor.for_path(path)  
    writer, _ = client.do_put(descriptor, table.schema)  
    writer.write_table(table)  
    writer.close()

def commit_ingest(  
    uri: str,  
    staging_name: str,  
    target_table: str,  
    token: str \| None = None,  
    ca_path: str \| None = None,  
):  
    client = connect_client(uri, token=token, ca_path=ca_path)  
    body = json.dumps({"staging": staging_name, "table": target_table}).encode()  
    action = flight.Action("commit_ingest", body)  
    results = list(client.do_action(action))  
    if not results:  
        return {}  
    return json.loads(results[0].body.to_pybytes().decode())

def list_staging(uri: str, token: str \| None = None, ca_path: str \| None = None):  
    client = connect_client(uri, token=token, ca_path=ca_path)  
    action = flight.Action("list_staging", b"")  
    results = list(client.do_action(action))  
    if not results:  
        return {}  
    return json.loads(results[0].body.to_pybytes().decode())

def prepared_demo(uri: str, token: str \| None = None, ca_path: str \| None = None) -> pa.Table:  
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
```

## 4.12 src/transport/benchmark.py

```python  
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
    token: str \| None = None,  
    ca_path: str \| None = None,  
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
```

## 4.13 src/flight_sql/adbc_client.py

```python  
from __future__ import annotations

import argparse  
import json  
import os

import adbc_driver_flightsql.dbapi as flight_sql

def connect(  
    uri: str,  
    token: str \| None = None,  
    ca_path: str \| None = None,  
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
    token: str \| None = None,  
    ca_path: str \| None = None,  
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
```

## 4.14 scripts/create_db.sql

```sql  
-- Run as a PostgreSQL superuser or database owner.  
CREATE DATABASE flight_demo;  
```

# 5. Runbook

## 5.1 Prerequisites

You need:

* Python 3.11+  
* PostgreSQL running locally or reachable  
* RustFS running and S3-compatible endpoint reachable  
* OpenSSL for self-signed TLS certs

Example versions used in this design:

* PostgreSQL 15+  
* Python 3.11  
* PyArrow 17+  
* RustFS with S3 API on http://localhost:9000

---

## 5.2 Create PostgreSQL database

Option A: using ```createdb```  
```bash  
createdb flight_demo  
```

Option B: using ```psql```  

```bash  
psql -U postgres -h localhost -p 5432 -f scripts/create_db.sql  
```

Then ensure your ```.env``` has the correct credentials.  

Example:  

```dotenv  
PGHOST=localhost  
PGPORT=5432  
PGDATABASE=flight_demo  
PGUSER=postgres  
PGPASSWORD=postgres  
PGSCHEMA=flight_demo  
```

## 5.3 Create Python virtual environment

```bash  
cd arrow-flight-postgres-demo

python -m venv .venv  
source .venv/bin/activate

pip install -r requirements.txt  
```

On Windows PowerShell:  

```powershell  
python -m venv .venv  
.venv\Scripts\Activate.ps1  
pip install -r requirements.txt  
```

## 5.4 Configure RustFS

Ensure RustFS is running. Example expected settings:  

```dotenv  
RUSTFS_ENDPOINT=http://localhost:9000  
RUSTFS_ACCESS_KEY=rustfsadmin  
RUSTFS_SECRET_KEY=rustfsadmin  
RUSTFS_BUCKET=arrow-flight-demo  
RUSTFS_REGION=us-east-1  
```

If your RustFS credentials differ, update ```.env```.  

You can test RustFS connectivity later with:  
```bash  
python -m src.storage.rustfs --list --prefix ""  
```

## 5.5 Generate synthetic data


Start with a small dataset to validate everything:  

```bash  
python -m src.etl.generate_synthetic \  
  --events-rows 200000 \
  --sales-rows 100000 \  
  --batch-size 50000 \  
  --upload  
```

This creates:  

```text  
data/csv/events.csv  
data/csv/sales.csv  
data/parquet/events/part-0000.parquet  
data/parquet/sales/part-0000.parquet  
data/manifest.json  
```

And, if RustFS is configured and ```--upload``` is passed, uploads:  

```text  
s3://arrow-flight-demo/demo/events/part-0000.parquet  
s3://arrow-flight-demo/demo/sales/part-0000.parquet  
```

Verify RustFS objects:  
```bash  
python -m src.storage.rustfs --list --prefix demo/  
```

For a larger, article-like workload:  
```bash  
python -m src.etl.generate_synthetic \  
  --events-rows 10000000 \
  --sales-rows 5000000 \
  --batch-size 100000 \
  --upload
```
Warning: 10M rows can take significant time, disk, and memory. Start small first.  


## 5.6 Load synthetic data into PostgreSQL

```bash  
python -m src.etl.load_postgres --manifest data/manifest.json  
```

This:

* creates schema ```flight_demo```  
* creates tables:  
  * ```flight_demo.events```  
  * ```flight_demo.sales```  
  * ```flight_demo.flight_upload_demo```  
* truncates them  
* loads CSV data using PostgreSQL ```COPY```  
* runs ```ANALYZE```


Validate:  

```bash  
psql -U postgres -h localhost -d flight_demo -c "SET search_path TO flight_demo; SELECT count(*) FROM events;"  
psql -U postgres -h localhost -d flight_demo -c "SET search_path TO flight_demo; SELECT count(*) FROM sales;"  
```

# 6. Run Arrow Flight server

## 6.1 Minimal plaintext server

Terminal 1:  
```bash  
python -m src.flight.server_minimal --port 8815 --no-auth  
```
This starts a Flight server on:  
```text  
grpc://0.0.0.0:8815  
```

Clients should use:  

```text  
grpc://localhost:8815  
```

## 6.2 Test a simple Flight read

Terminal 2:  
```bash  
python -m src.flight.client \  
  --uri grpc://localhost:8815 \  
  --sql "SELECT * FROM events WHERE day='2026-08-01' LIMIT 100"  
```
Expected output includes:

* row count  
* Arrow schema  
* first few rows as Python dict


## 6.3 Read a named dataset by path

```bash  
python -m src.flight.client \  
  --uri grpc://localhost:8815 \  
  --path events  
```
This uses:  
```python  
FlightDescriptor.for_path("events")  
```

## 6.4 List available flights

```bash  
python -m src.flight.client \  
  --uri grpc://localhost:8815 \  
  --list-flights  
```

# 7. Benchmark JDBC/ODBC-like row transport vs Arrow Flight

The article’s core point is:  
```Query time may be small; transport time may dominate.```  

This project uses PostgreSQL’s Python DB-API row cursor as a practical stand-in for JDBC/ODBC row-oriented transport.  

Start the minimal Flight server if it is not already running:  
```bash  
python -m src.flight.server_minimal --port 8815 --no-auth  
```

Then run:  
```bash  
python -m src.transport.benchmark \  
  --flight-uri grpc://localhost:8815 \  
  --query "SELECT * FROM events WHERE day='2026-08-01'" \  
  --fetch-sizes 1000 10000 50000  
```
What you should see:

* PostgreSQL engine time from ```EXPLAIN ANALYZE```  
* row-oriented fetch wall-clock times  
* Arrow Flight wall-clock time  
* rows/sec comparison

Interpretation:

* The row path pays per-cell fetch/materialization overhead.  
* The Flight path receives Arrow IPC columnar batches.  
* On larger result sets, the Flight advantage grows.

If you want to avoid pandas materialization in the row path:  
```bash  
python -m src.transport.benchmark \  
  --flight-uri grpc://localhost:8815 \  
  --query "SELECT * FROM events WHERE day='2026-08-01'" \  
  --no-materialize  
```

# 8. Parallel endpoints

Arrow Flight’s parallelism comes from returning multiple endpoints in ```FlightInfo```.  

Start a sharded server:  
```bash  
python -m src.flight.server_sharded \  
  --port 8815 \  
  --shards 4 \  
  --no-auth  
```

Now a single ```GetFlightInfo``` returns 4 endpoints.  

Run a parallel client read:  

```bash  
python -m src.flight.client \  
  --uri grpc://localhost:8815 \  
  --parallel \  
  --sql "SELECT * FROM events WHERE day='2026-08-01'"  
```
The client:

1. calls ```get_flight_info```  
2. receives 4 endpoints  
3. spawns 4 concurrent ```do_get``` streams  
4. concatenates the resulting Arrow tables

You can also benchmark parallel Flight:  
```bash  
python -m src.transport.benchmark \  
  --flight-uri grpc://localhost:8815 \  
  --parallel-flight \  
  --query "SELECT * FROM events WHERE day='2026-08-01'"  
```

# 9. Distributed coordinator / worker pattern

This demonstrates the article’s key distributed-read idea:

* coordinator plans the query  
* coordinator returns endpoints pointing to workers  
* workers stream data directly to client  
* coordinator moves metadata only, not bulk data

## 9.1 Start worker 1

Terminal 1:  
```bash  
python -m src.flight.server_sharded \  
  --port 8816 \  
  --shards 1 \  
  --no-auth
```
## 9.2 Start worker 2

Terminal 2:  
```bash  
python -m src.flight.server_sharded \  
  --port 8817 \  
  --shards 1 \  
  --no-auth
```

## 9.3 Start coordinator

Terminal 3:  
```bash  
python -m src.flight.server_sharded \  
  --port 8815 \  
  --shards 4 \  
  --workers grpc://localhost:8816 grpc://localhost:8817 \  
  --no-auth  
```
The coordinator returns 4 endpoints:

* endpoint 0 -> worker 8816  
* endpoint 1 -> worker 8817  
* endpoint 2 -> worker 8816  
* endpoint 3 -> worker 8817

## 9.4 Client fans out to workers

Terminal 4:  
```bash  
python -m src.flight.client \  
  --uri grpc://localhost:8815 \  
  --parallel \  
  --sql "SELECT * FROM events WHERE day='2026-08-01'"  
```

The client contacts the coordinator once for metadata, then reads directly from workers.  
---

# 10. TLS and bearer-token authentication

## 10.1 Generate self-signed certificates

From project root:  
```bash  
mkdir -p certs

openssl req -x509 -newkey rsa:4096 -sha256 -days 365 -nodes \  
  -keyout certs/server.key \  
  -out certs/server.crt \  
  -subj "/CN=localhost" \  
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

cp certs/server.crt certs/ca.pem  
chmod 600 certs/server.key  
```

Set a token in ```.env``` or pass it on the command line:  
```bash  
export FLIGHT_AUTH_TOKEN=demo-secret-token  
```

## 10.2 Start secure Flight server

```bash  
python -m src.flight.server_minimal \  
  --port 8815 \  
  --tls \  
  --token "$FLIGHT_AUTH_TOKEN"  
```
Server listens on:  
```text  
grpc+tls://0.0.0.0:8815  
```

## 10.3 Secure client read

```bash  
python -m src.flight.client \  
  --uri grpc+tls://localhost:8815 \  
  --ca certs/ca.pem \  
  --token "$FLIGHT_AUTH_TOKEN" \  
  --sql "SELECT * FROM events WHERE day='2026-08-01' LIMIT 100"  
```
What this proves:

* auth is enforced in middleware, not inside each method  
* TLS encrypts Arrow buffers on the wire  
* client injects bearer token using client middleware



## 10.4 Negative auth test

Bad token should fail:  
```bash  
python -m src.flight.client \  
  --uri grpc+tls://localhost:8815 \  
  --ca certs/ca.pem \  
  --token wrong-token \  
  --sql "SELECT 1"  
```

Expected:  
```text  
FlightUnauthenticatedError  
```

# 11. DoPut ingest and DoAction commit

This demonstrates the write path:

* client uploads Arrow record batches using ```DoPut```  
* server stages them in memory  
* client triggers ```DoAction("commit_ingest")```  
* server inserts staged batches into PostgreSQL


Make sure a plaintext server is running:  

```bash  
python -m src.flight.server_minimal --port 8815 --no-auth  
```

Then upload a slice of generated Parquet:  
```bash  
python -m src.flight.client \  
  --uri grpc://localhost:8815 \  
  --put-parquet data/parquet/events/part-0000.parquet \  
  --put-path upload_demo \  
  --put-rows 10000 \  
  --commit-table flight_upload_demo  
```
Check PostgreSQL:  
```bash  
psql -U postgres -h localhost -d flight_demo -c "SET search_path TO flight_demo; SELECT count(*) FROM flight_upload_demo;"  
```

List staged datasets:  
```bash  
python -m src.flight.client \  
  --uri grpc://localhost:8815 \  
  --list-staging  
```

# 12. Prepared statement demo over Flight

This is not official Flight SQL Protobuf, but it demonstrates the same protocol shape:

* create prepared statement using DoAction  
* bind parameters using DoPut  
* execute using GetFlightInfo + DoGet

Start server:  
```bash  
python -m src.flight.server_minimal --port 8815 --no-auth  
```

Run:  
```bash  
python -m src.flight.client \  
  --uri grpc://localhost:8815 \  
  --prepared-demo  
```

The demo executes:  
```sql  
SELECT id, kind, amount  
FROM events  
WHERE day=%s AND kind=%s  
```

with bound parameters:  


```python  
(date(2026, 8, 1), "purchase")  
```

# 13. Catalog metadata demo

The Flight-SQL-style server supports metadata commands:

* ```get_catalogs```  
* ```get_db_schemas```  
* ```get_tables```

Start server:  

```bash  
python -m src.flight.server_minimal --port 8815 --no-auth  
```

Run:  
```bash  
python -m src.flight.client \  
  --uri grpc://localhost:8815 \  
  --metadata get_tables  
```

Also:

```bash  
python -m src.flight.client \  
  --uri grpc://localhost:8815 \  
  --metadata get_db_schemas  
```

# 14. Official ADBC Flight SQL client path

The runnable server in this project uses JSON command envelopes over raw Flight to demonstrate Flight SQL concepts without implementing the full official Protobuf protocol.  
For real Flight SQL, use the ADBC client module against a compliant Flight SQL server.  
Examples of real Flight SQL servers:

* Dremio Flight SQL endpoint  
* InfluxDB 3.x / IOx Flight SQL endpoint  
* Ballista  
* an ADBC Flight SQL server wrapping another ADBC driver

Set environment variables:  
```bash  
export FLIGHT_SQL_URI=grpc+tls://your-flightsql-server:32010  
export FLIGHT_SQL_TOKEN="Bearer your-token"  
export FLIGHT_SQL_CA=/path/to/ca.pem  
```
Run:  
```bash  
python -m src.flight_sql.adbc_client \  
  --uri "$FLIGHT_SQL_URI" \  
  --sql "SELECT region, sum(amount) AS total FROM sales GROUP BY region" \  
  --token "$FLIGHT_SQL_TOKEN" \  
  --ca "$FLIGHT_SQL_CA"  
```

Parameterized query example:  
```bash  
python -m src.flight_sql.adbc_client \  
  --uri "$FLIGHT_SQL_URI" \  
  --sql "SELECT id, kind, ts FROM events WHERE day=? AND kind=?" \  
  --params '["2026-08-01", "purchase"]' \  
  --token "$FLIGHT_SQL_TOKEN" \  
  --ca "$FLIGHT_SQL_CA"  
```
This is the production-style JDBC/ODBC replacement path:

* Python analytics: ADBC Flight SQL driver  
* BI tools: ODBC-over-Flight-SQL shim  
* Java services: JDBC-over-Flight-SQL driver

---

# 15. Concept coverage map

| Article concept | Where it is covered in this project |
| ----- | ----- |
| JDBC/ODBC row-major serialization tax | src/transport/benchmark.py compares PostgreSQL row cursor fetch vs Flight |
| Engine time vs transport time decomposition | explain_analyze_time_ms() and benchmark output |
| Arrow IPC columnar wire | Flight server streams RecordBatch objects via GeneratorStream |
| Zero-deserialization receive | Client uses do_get(...).read_all() to produce pyarrow.Table |
| gRPC control plane / Arrow data plane | get_flight_info() returns metadata; do_get() streams data |
| FlightDescriptor | for_command(sql) and for_path(dataset) used in server/client |
| FlightInfo | Returned by get_flight_info() |
| Ticket | Opaque JSON ticket used by do_get() |
| Endpoint | One or more endpoints returned by server |
| Location | Worker/coordinator URIs in endpoints |
| DoGet | Read path in server_minimal.py and server_sharded.py |
| DoPut | Ingest path and prepared parameter binding |
| DoAction | Prepared statements, transactions, commit ingest, list staging |
| ListFlights | list_flights() in server and client |
| Parallel endpoints | server_sharded.py + read_parallel_uri() |
| Distributed reads without coordinator funnel | coordinator/worker runbook in section 9 |
| Flight SQL standardization idea | JSON command envelopes for query/catalog/prepared/transaction |
| ADBC Flight SQL client | src/flight_sql/adbc_client.py |
| Prepared statements | prepared_demo() in client and server actions |
| Parameter binding | DoPut with Arrow parameter batch |
| Catalog metadata | get_catalogs, get_db_schemas, get_tables |
| Auth middleware | src/flight/auth.py |
| TLS | --tls, grpc+tls://, tls_certificates, tls_root_certs |
| Streaming large results | GeneratorStream + PostgreSQL server-side cursor |
| RustFS object storage | Generated Parquet uploaded via src/storage/rustfs.py |
| Synthetic data generation | src/etl/generate_synthetic.py |
| PostgreSQL loading | src/etl/load_postgres.py |

---

# 16. Recommended test sequence

From project root, with PostgreSQL and RustFS running:  

```bash  
# 1. Activate env  
source .venv/bin/activate

# 2. Create DB if needed  
createdb flight_demo

# 3. Generate small data  
python -m src.etl.generate_synthetic \  
  --events-rows 200000 \  
  --sales-rows 100000 \  
  --batch-size 50000 \  
  --upload

# 4. Load PostgreSQL  
python -m src.etl.load_postgres --manifest data/manifest.json

# 5. Start minimal Flight server in another terminal  
python -m src.flight.server_minimal --port 8815 --no-auth

# 6. Basic Flight read  
python -m src.flight.client \  
  --uri grpc://localhost:8815 \  
  --sql "SELECT * FROM events WHERE day='2026-08-01' LIMIT 100"

# 7. Benchmark row vs Flight  
python -m src.transport.benchmark \  
  --flight-uri grpc://localhost:8815 \  
  --query "SELECT * FROM events WHERE day='2026-08-01'" \  
  --fetch-sizes 1000 10000 50000

# 8. Parallel endpoints  
python -m src.flight.server_sharded --port 8815 --shards 4 --no-auth

python -m src.flight.client \  
  --uri grpc://localhost:8815 \  
  --parallel \  
  --sql "SELECT * FROM events WHERE day='2026-08-01'"

# 9. TLS + auth  
openssl req -x509 -newkey rsa:4096 -sha256 -days 365 -nodes \  
  -keyout certs/server.key \  
  -out certs/server.crt \  
  -subj "/CN=localhost" \  
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

cp certs/server.crt certs/ca.pem

export FLIGHT_AUTH_TOKEN=demo-secret-token

python -m src.flight.server_minimal \  
  --port 8815 \  
  --tls \  
  --token "$FLIGHT_AUTH_TOKEN"

python -m src.flight.client \  
  --uri grpc+tls://localhost:8815 \  
  --ca certs/ca.pem \  
  --token "$FLIGHT_AUTH_TOKEN" \  
  --sql "SELECT * FROM events WHERE day='2026-08-01' LIMIT 100"

# 10. DoPut ingest  
python -m src.flight.client \  
  --uri grpc://localhost:8815 \  
  --put-parquet data/parquet/events/part-0000.parquet \  
  --put-path upload_demo \  
  --put-rows 10000 \  
  --commit-table flight_upload_demo

# 11. Prepared demo  
python -m src.flight.client \  
  --uri grpc://localhost:8815 \  
  --prepared-demo

# 12. Metadata demo  
python -m src.flight.client \  
  --uri grpc://localhost:8815 \  
  --metadata get_tables  
```

# 17. Troubleshooting

## PostgreSQL connection fails

Check:  
```bash  
psql -U postgres -h localhost -p 5432 -d flight_demo -c "SELECT 1;"
```  

Verify ```.env```:  

```dotenv  
PGHOST=localhost  
PGPORT=5432  
PGDATABASE=flight_demo  
PGUSER=postgres  
PGPASSWORD=postgres  
```

## RustFS upload fails

Check endpoint:  
```bash  
curl http://localhost:9000/minio/health/live  
```
Or simply:  
```bash  
curl -v http://localhost:9000  
```

Verify credentials in ```.env```.  

Test listing:  

```bash  
python -m src.storage.rustfs --list --prefix ""  
```

## Flight client cannot connect

Ensure server is running and URI matches TLS setting:

* plaintext: ```grpc://localhost:8815```  
* TLS: ```grpc+tls://localhost:8815```


If TLS server uses self-signed cert, pass CA:  
```bash  
--ca certs/ca.pem  
```

## Auth fails

Ensure client token exactly matches server token:  

```bash  
--token "$FLIGHT_AUTH_TOKEN"  
```
Header must be:  
```text  
Authorization: Bearer <token>  
```

The client middleware injects this automatically.  


## Parallel read fails with “column id does not exist”

The sharded server partitions using:  
```sql  
mod(id, N) = partition_index  
```

So the base query must expose an id column.  
The bundled datasets do:  
```sql  
SELECT id, ... FROM events  
SELECT id, ... FROM sales  
```

If you supply a custom query, include id.  


## ADBC Flight SQL client fails against local Flight server

That is expected.  
The local server is raw Flight / Flight-SQL-style JSON, not official Flight SQL Protobuf.  
Use ```src.flight_sql.adbc_client``` only against a real Flight SQL server.  


---

# 18. What to scale up for article-like benchmarks

For the article’s “10M rows” style example:  

```bash  
python -m src.etl.generate_synthetic \  
  --events-rows 10000000 \  
  --sales-rows 5000000 \  
  --batch-size 100000 \  
  --upload

python -m src.etl.load_postgres  
```

Then benchmark:  
```bash  
python -m src.transport.benchmark \  
  --flight-uri grpc://localhost:8815 \  
  --query "SELECT * FROM events WHERE day='2026-08-01'" \  
  --fetch-sizes 1000 10000 50000 100000  
```

For parallel Flight:  
```bash  
python -m src.flight.server_sharded --port 8815 --shards 8 --no-auth

python -m src.transport.benchmark \  
  --flight-uri grpc://localhost:8815 \  
  --parallel-flight \  
  --query "SELECT * FROM events WHERE day='2026-08-01'"  
```

You should observe:

* PostgreSQL engine time remains relatively stable.  
* Row-oriented transport dominates wall-clock.  
* Flight transport collapses the overhead.  
* Parallel endpoints improve ability to saturate network/client cores.

---

# 19. Production notes

This project is designed as a learning/interview/demo system. For production:

1. Replace JSON command envelopes with official Flight SQL Protobuf if you need third-party Flight SQL drivers.  
2. Use a real Flight SQL server or implement the official protocol.  
3. Put PostgreSQL behind a proper query engine if you need true analytical columnar execution.  
4. Use managed TLS certificates, not self-signed.  
5. Validate JWTs, scopes, and row/column policies in middleware.  
6. Add observability:  
   * request tracing  
   * metrics for rows/sec, bytes/sec, endpoint count  
   * slow-query logging  
7. Tune Arrow batch size to balance memory and latency.  
8. Ensure network infrastructure supports long-lived HTTP/2 gRPC streams:  
   * no aggressive L7 idle timeouts  
   * HTTP/2 end-to-end  
   * gRPC-aware load balancing if needed  
9. For very large PostgreSQL result sets, consider:  
   * server-side cursors  
   * partition pruning  
   * materialized views  
   * columnar acceleration layer  
   * direct Parquet/RustFS reads for analytical consumers

---

# 20. Minimal “proof it works” checklist

Run these in order:  
```bash  
python -m src.etl.generate_synthetic --events-rows 50000 --sales-rows 20000 --batch-size 10000 --upload  

python -m src.etl.load_postgres  

python -m src.flight.server_minimal --port 8815 --no-auth  

python -m src.flight.client --uri grpc://localhost:8815 --sql "SELECT * FROM events LIMIT 10"  

python -m src.transport.benchmark --flight-uri grpc://localhost:8815 --query "SELECT * FROM events WHERE day='2026-08-01'" --fetch-sizes 1000 10000  

python -m src.flight.server_sharded --port 8815 --shards 4 --no-auth  

python -m src.flight.client --uri grpc://localhost:8815 --parallel --sql "SELECT * FROM events WHERE day='2026-08-01'"  

python -m src.flight.client --uri grpc://localhost:8815 --prepared-demo  

python -m src.flight.client --uri grpc://localhost:8815 --metadata get_tables  

python -m src.storage.rustfs --list --prefix demo/  
```

If all of those succeed, the project is covering the article’s main mechanics on your local PostgreSQL + RustFS setup.  
