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