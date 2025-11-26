import io
import pytest
import pandas as pd
import polars as pl
from unittest.mock import MagicMock, patch

from flightrobustness.io.storage_adapters import (
    S3Adapter,
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
