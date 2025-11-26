import os
import polars as pl
import pytest
from pathlib import Path

from flightrobustness.io.file_reader import FileReader
from flightrobustness.io.file_writer import FileWriter
from flightrobustness.io.storage_adapters import LocalDiskAdapter
from flightrobustness.core.exceptions import DataSourceError


def test_read_write_csv_safe(tmp_path):
    """Ensure that CSV writing and reading works properly."""
    df = pl.DataFrame({"LegId": [1, 2], "STD": [600, 720], "STA": [700, 840]})
    path = tmp_path / "test.csv"

    writer = FileWriter()
    result = writer.write_csv(df, str(path))
    assert isinstance(result, (str, Path))
    assert os.path.exists(result)

    reader = FileReader()
    df2 = reader.read_csv(str(path), use_polars=True)
    assert df2.shape == df.shape
    assert df2["LegId"].to_list() == [1, 2]


def test_read_yaml_safe(tmp_path):
    """Test that YAML config reading works as expected."""
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text("mode: monte_carlo\nn_runs: 3\n")

    reader = FileReader()
    config = reader.read_yaml(str(yaml_path))
    assert config["mode"] == "monte_carlo"
    assert config["n_runs"] == 3


@pytest.fixture
def clean_local_storage(tmp_path, monkeypatch):
    """Fixture providing a clean LocalDiskAdapter in a temporary directory."""
    from flightrobustness.io.storage_adapters import LocalDiskAdapter
    adapter = LocalDiskAdapter()
    monkeypatch.setenv("LOCAL_DATA_DIR", str(tmp_path))
    # We don't change cwd to avoid messing up other tests
    return adapter


def test_file_reader_csv_empty_error(tmp_path):
    """Test that reading an empty CSV raises DataSourceError."""
    empty_csv = tmp_path / "empty.csv"
    empty_csv.touch()
    
    reader = FileReader()
    with pytest.raises(DataSourceError) as exc:
        reader.read_csv(str(empty_csv), use_polars=True)
    assert "Failed to read CSV" in str(exc.value)


def test_file_reader_csv_not_found():
    """Test that reading a non-existent CSV raises DataSourceError."""
    reader = FileReader()
    with pytest.raises(DataSourceError) as exc:
        reader.read_csv("non_existent.csv")
    assert "Failed to read CSV" in str(exc.value)


def test_file_reader_yaml_not_found():
    """Test that reading a non-existent YAML raises DataSourceError."""
    reader = FileReader()
    with pytest.raises(DataSourceError):
        reader.read_yaml("non_existent.yaml")


def test_file_reader_yaml_invalid_structure(tmp_path):
    """Test that reading an invalid YAML raises DataSourceError."""
    bad_yaml = tmp_path / "bad.yaml"
    bad_yaml.write_text("just_a_string")
    
    reader = FileReader()
    with pytest.raises(DataSourceError) as exc:
        reader.read_yaml(str(bad_yaml))
    assert "Invalid YAML structure" in str(exc.value)


def test_file_writer_empty_df_error(tmp_path):
    """Test that writing an empty DataFrame raises DataSourceError."""
    df = pl.DataFrame()
    writer = FileWriter()
    with pytest.raises(DataSourceError) as exc:
        writer.write_csv(df, str(tmp_path / "out.csv"))
    assert "Cannot write empty DataFrame" in str(exc.value)


def test_file_writer_exception(tmp_path):
    """Test that write errors are caught and raised as DataSourceError."""
    df = pl.DataFrame({"a": [1]})
    # Try to write to a directory path instead of a file
    writer = FileWriter()
    with pytest.raises(DataSourceError) as exc:
        writer.write_csv(df, str(tmp_path))
    assert "Failed to write file" in str(exc.value)

