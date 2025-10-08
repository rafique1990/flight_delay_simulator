from flightrobustness.io.storage_adapters import get_storage

class FileReader:
    """ supports local or S3 storage via adapters."""

    @staticmethod
    def read_csv(path: str, use_polars: bool = False):
        """
        Reads a CSV file via the configured storage adapter.
        """
        storage = get_storage()
        try:
            df = storage.read_csv(path, use_polars=use_polars)
            if df is None or (hasattr(df, "is_empty") and df.is_empty()):
                raise ValueError(f"Empty or invalid data: {path}")
            return df
        except Exception as e:
            raise RuntimeError(f"Failed to read CSV: {path} ({e})") from e

    @staticmethod
    def read_yaml(path: str) -> dict:
        """
        Reads a YAML config file only from local disk.YAML config is not expected to live in S3.
        """
        import os, yaml
        if not os.path.exists(path):
            raise FileNotFoundError(f"YAML file not found: {path}")
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        if not isinstance(data, dict):
            raise ValueError(f"Invalid YAML structure: {path}")
        return data
