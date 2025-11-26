from typing import Any, Dict
import polars as pl
from flightrobustness.io.storage_adapters import get_storage
from flightrobustness.core.exceptions import DataSourceError

class FileReader:
    """
    CSV and YAML file reader with storage adapter support.
    Implements ScheduleRepository protocol.
    """
    
    def __init__(self, storage=None):
        self.storage = storage or get_storage()

    def load_schedule(self, source: str) -> pl.DataFrame:
        """Read flight schedule from CSV."""
        return self.read_csv(source, use_polars=True)

    def read_csv(self, path: str, use_polars: bool = False) -> Any:
        """Read CSV file using configured storage backend."""
        try:
            df = self.storage.read_csv(path, use_polars=use_polars)
            if df is None or (hasattr(df, "is_empty") and df.is_empty()):
                raise DataSourceError(f"Empty or invalid data: {path}")
            return df
        except Exception as e:
            raise DataSourceError(f"Failed to read CSV: {path} ({e})") from e

    def read_yaml(self, path: str) -> Dict[str, Any]:
        """Read YAML configuration from local filesystem."""
        import os, yaml
        if not os.path.exists(path):
            raise DataSourceError(f"YAML file not found: {path}")
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
            if not isinstance(data, dict):
                raise DataSourceError(f"Invalid YAML structure: {path}")
            return data
        except Exception as e:
            raise DataSourceError(f"Failed to read YAML: {path} ({e})") from e
