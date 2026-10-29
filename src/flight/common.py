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
            host, port = rest.rsplit(":", 1)
            return flight.Location.for_grpc_tls(host, int(port))
        if uri.startswith("grpc://"):
            rest = uri.split("://", 1)[1]
            host, port = rest.rsplit(":", 1)
            return flight.Location.for_grpc_tcp(host, int(port))
        return uri


def pg_oid_to_arrow(oid: int | None) -> pa.DataType:
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