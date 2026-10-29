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


def upload_file(settings: Settings, local_path: Path | str, key: str) -> str | None:
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