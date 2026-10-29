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