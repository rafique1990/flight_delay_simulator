import os, io
import pandas as pd
import polars as pl
from typing import Union, Tuple

try:
    import boto3
    S3_AVAILABLE = True
except ImportError:
    S3_AVAILABLE = False

STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "local").lower()
LOCAL_DATA_DIR = os.getenv("LOCAL_DATA_DIR", "data")
S3_INPUT_BUCKET = os.getenv("S3_INPUT_BUCKET")
S3_OUTPUT_BUCKET = os.getenv("S3_OUTPUT_BUCKET")
AWS_REGION = os.getenv("AWS_REGION", "eu-central-1")


class StorageAdapterError(Exception):
    pass


class LocalDiskAdapter:
    """Handles file I/O on the local filesystem."""

    def _get_local_path(self, path: str) -> str:
        """
        Safely resolve both absolute and relative paths.
        - If `path` is absolute, return as-is.
        - If path already begins under LOCAL_DATA_DIR, avoid double prefixing e.g data/data/results.
        """
        # Convert to normalized path object
        path = os.path.normpath(path)
        base = os.path.normpath(LOCAL_DATA_DIR)

        # Case 1: Absolute path, return directly
        if os.path.isabs(path):
            return path

        # Case 2: Already starts with LOCAL_DATA_DIR (avoid double-prepending data/data/results)
        if path.startswith(base):
            return path

        # Case 3: Relative → join with base
        return os.path.join(base, path)

    def read_csv(self, path: str, use_polars=False, **kwargs):
        full_path = self._get_local_path(path)
        if use_polars:
            return pl.read_csv(full_path, **kwargs)
        return pd.read_csv(full_path, **kwargs)

    def write_csv(self, df: Union[pd.DataFrame, pl.DataFrame], path: str, **kwargs):
        full_path = self._get_local_path(path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        if isinstance(df, pl.DataFrame):
            df.write_csv(full_path, **kwargs)
        else:
            df.to_csv(full_path, index=False, **kwargs)
        return full_path


##TODO: Test it after the deployment on AWS S3.
class S3Adapter:
    """Basic S3 wrapper using boto3."""
    def __init__(self):
        if not S3_AVAILABLE:
            raise StorageAdapterError("boto3 not installed")
        self.s3 = boto3.client("s3", region_name=AWS_REGION)

    def _bucket_key(self, path: str, read=False) -> Tuple[str, str]:
        bucket = S3_INPUT_BUCKET if read else S3_OUTPUT_BUCKET
        if not bucket:
            raise StorageAdapterError("S3 bucket not configured in ENV.")
        return bucket, path.lstrip("./")

    def read_csv(self, path: str, use_polars=False, **kwargs):
        bucket, key = self._bucket_key(path, read=True)
        obj = self.s3.get_object(Bucket=bucket, Key=key)
        buf = io.BytesIO(obj["Body"].read())
        return pl.read_csv(buf, **kwargs) if use_polars else pd.read_csv(buf, **kwargs)

    def write_csv(self, df: Union[pd.DataFrame, pl.DataFrame], path: str, **kwargs):
        bucket, key = self._bucket_key(path)
        buf = io.BytesIO()
        df.write_csv(buf, **kwargs) if isinstance(df, pl.DataFrame) else df.to_csv(buf, index=False, **kwargs)
        buf.seek(0)
        self.s3.upload_fileobj(buf, bucket, key)
        return f"s3://{bucket}/{key}"


def get_storage():
    return S3Adapter() if STORAGE_BACKEND == "s3" else LocalDiskAdapter()
