from typing import Union
import pandas as pd
import polars as pl
from flightrobustness.io.storage_adapters import get_storage
from flightrobustness.core.exceptions import DataSourceError

class FileWriter:
    """
    DataFrame writer with local and S3 backend support.
    Implements ResultRepository protocol.
    """
    
    def __init__(self, storage=None):
        self.storage = storage or get_storage()

    def save_results(self, df: pl.DataFrame, path: str) -> str:
        """Save results to CSV."""
        return self.write_csv(df, path)

    def write_csv(self, df: Union[pd.DataFrame, pl.DataFrame], path: str, **kwargs) -> str:
        """Write DataFrame to CSV using configured storage backend."""
        if df is None or (hasattr(df, "is_empty") and df.is_empty()):
            raise DataSourceError("Cannot write empty DataFrame.")
        try:
            final_path = self.storage.write_csv(df, path, **kwargs)
            print(f"Written successfully: {final_path}")
            return final_path
        except Exception as e:
            raise DataSourceError(f"Failed to write file: {path} ({e})") from e
