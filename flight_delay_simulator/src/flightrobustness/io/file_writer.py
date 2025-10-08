from flightrobustness.io.storage_adapters import get_storage
from typing import Union
import pandas as pd
import polars as pl

class FileWriter:
    """handles local or S3 outputs."""

    @staticmethod
    def write_csv(df: Union[pd.DataFrame, pl.DataFrame], path: str, **kwargs) -> str:
        """
        Writes a DataFrame using the active storage backend (LocalDisk or S3) and returns the final path (local or s3://...).
        """
        storage = get_storage()
        if df is None or (hasattr(df, "is_empty") and df.is_empty()):
            raise ValueError("Cannot write empty DataFrame.")
        try:
            final_path = storage.write_csv(df, path, **kwargs)
            print(f"Written successfully: {final_path}")
            return final_path
        except Exception as e:
            raise RuntimeError(f"Failed to write file: {path} ({e})") from e
