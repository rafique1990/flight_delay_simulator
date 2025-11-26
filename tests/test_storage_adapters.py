import io
import os
import pytest
import pandas as pd
import polars as pl
from unittest.mock import MagicMock, patch

from flightrobustness.io.storage_adapters import (
    S3Adapter,
    LocalDiskAdapter,
    StorageAdapterError,
    get_storage,
)

@pytest.fixture
def mock_boto3(monkeypatch):
    """Provide mocked boto3 client to avoid real AWS network calls."""
    mock_client = MagicMock()
    monkeypatch.setattr(
        "flightrobustness.io.storage_adapters.boto3.client",
        lambda *a, **kw: mock_client,
    )
    return mock_client


def test_s3_read_csv_with_pandas(monkeypatch, mock_boto3):
    """Test reading CSV via pandas with proper module-level patching."""
    monkeypatch.setattr("flightrobustness.io.storage_adapters.S3_INPUT_BUCKET", "in-bucket")
    monkeypatch.setattr("flightrobustness.io.storage_adapters.S3_OUTPUT_BUCKET", "out-bucket")
    mock_boto3.get_object.return_value = {"Body": io.BytesIO(b"col1,col2\n1,2\n3,4")}
    adapter = S3Adapter()
    df = adapter.read_csv("file.csv")
    assert not df.empty
    assert list(df.columns) == ["col1", "col2"]


def test_s3_read_csv_with_polars(monkeypatch, mock_boto3):
    """Test reading CSV via polars with proper module-level patching."""
    monkeypatch.setattr("flightrobustness.io.storage_adapters.S3_INPUT_BUCKET", "in-bucket")
    monkeypatch.setattr("flightrobustness.io.storage_adapters.S3_OUTPUT_BUCKET", "out-bucket")
    mock_boto3.get_object.return_value = {"Body": io.BytesIO(b"col1,col2\n1,2\n3,4")}
    adapter = S3Adapter()
    df = adapter.read_csv("file.csv", use_polars=True)
    assert isinstance(df, pl.DataFrame)
    assert "col1" in df.columns


def test_s3_write_csv_with_pandas(monkeypatch, mock_boto3):
    """Test writing pandas DataFrame to S3 with patched buckets."""
    monkeypatch.setattr("flightrobustness.io.storage_adapters.S3_OUTPUT_BUCKET", "out-bucket")
    monkeypatch.setattr("flightrobustness.io.storage_adapters.S3_INPUT_BUCKET", "in-bucket")
    adapter = S3Adapter()
    df = pd.DataFrame({"a": [1]})
    path = adapter.write_csv(df, "out.csv")
    assert path == "s3://out-bucket/out.csv"
    mock_boto3.upload_fileobj.assert_called_once()


def test_s3_write_csv_with_polars(monkeypatch, mock_boto3):
    """Test writing polars DataFrame to S3 with patched buckets."""
    monkeypatch.setattr("flightrobustness.io.storage_adapters.S3_OUTPUT_BUCKET", "out-bucket")
    monkeypatch.setattr("flightrobustness.io.storage_adapters.S3_INPUT_BUCKET", "in-bucket")
    adapter = S3Adapter()
    df = pl.DataFrame({"a": [1]})
    path = adapter.write_csv(df, "out.csv")
    assert path == "s3://out-bucket/out.csv"
    mock_boto3.upload_fileobj.assert_called_once()


def test_get_storage_returns_s3(monkeypatch):
    """Ensure get_storage() correctly returns mocked S3Adapter."""
    monkeypatch.setattr("flightrobustness.io.storage_adapters.STORAGE_BACKEND", "s3")
    with patch("flightrobustness.io.storage_adapters.S3Adapter", return_value="mock_s3") as mock_class:
        adapter = get_storage()
        mock_class.assert_called_once()
        assert adapter == "mock_s3"


def test_local_disk_adapter_path_resolution(monkeypatch, tmp_path):
    """Test LocalDiskAdapter path resolution logic."""
    monkeypatch.setattr("flightrobustness.io.storage_adapters.LOCAL_DATA_DIR", str(tmp_path))
    adapter = LocalDiskAdapter()
    
    # Case 1: Absolute path
    abs_path = str(tmp_path / "abs/file.csv")
    assert adapter._get_local_path(abs_path) == abs_path
    
    # Case 2: Relative path
    rel_path = "rel/file.csv"
    expected = str(tmp_path / "rel/file.csv")
    assert adapter._get_local_path(rel_path) == expected
    
    # Case 3: Already starts with LOCAL_DATA_DIR (simulated)
    joined_path = str(tmp_path / "data/file.csv")
    assert adapter._get_local_path(joined_path) == joined_path


def test_s3_adapter_missing_boto3(monkeypatch):
    """Test S3Adapter raises error if boto3 is missing."""
    monkeypatch.setattr("flightrobustness.io.storage_adapters.S3_AVAILABLE", False)
    with pytest.raises(StorageAdapterError) as exc:
        S3Adapter()
    assert "boto3 not installed" in str(exc.value)


def test_s3_adapter_missing_bucket(monkeypatch, mock_boto3):
    """Test S3Adapter raises error if bucket is not configured."""
    monkeypatch.setattr("flightrobustness.io.storage_adapters.S3_INPUT_BUCKET", None)
    monkeypatch.setattr("flightrobustness.io.storage_adapters.S3_OUTPUT_BUCKET", None)
    
    adapter = S3Adapter()
    with pytest.raises(StorageAdapterError) as exc:
        adapter.read_csv("file.csv")
    assert "S3 bucket not configured" in str(exc.value)
