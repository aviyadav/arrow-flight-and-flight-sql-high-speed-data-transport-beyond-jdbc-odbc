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

TARGET_DAY = date(2026, 8, 1)
START_DAY = date(2026, 7, 1)
END_DAY = date(2026, 8, 31)
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
        amount = round(rng.uniform(1.0, 500.0), 2) if kind in {"purchase", "refund"} else 0.0
        yield (i, day, ts, user_id, kind, payload, amount)


def make_sales_rows(start_id: int, n_rows: int, rng: random.Random):
    for offset in range(n_rows):
        i = start_id + offset
        day = random_day(rng)
        ts = random_ts(rng, day)
        region = rng.choice(REGIONS)
        amount = round(rng.uniform(10.0, 10_000.0), 2)
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
        writer = csv.writer(f, lineterminator="\n")
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