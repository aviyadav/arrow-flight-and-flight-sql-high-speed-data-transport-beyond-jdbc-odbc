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