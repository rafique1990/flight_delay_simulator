import os
import polars as pl
import pytest
from pathlib import Path

from flightrobustness.io.file_reader import FileReader
from flightrobustness.io.file_writer import FileWriter
from flightrobustness.io.storage_adapters import LocalDiskAdapter


def test_read_write_csv_safe(tmp_path):
    """Ensure that CSV writing and reading works properly."""
    df = pl.DataFrame({"LegId": [1, 2], "STD": [600, 720], "STA": [700, 840]})
    path = tmp_path / "test.csv"

    result = FileWriter.write_csv(df, str(path))
    assert isinstance(result, (str, Path))
    assert os.path.exists(result)

    df2 = FileReader.read_csv(str(path), use_polars=True)
    assert df2.shape == df.shape
    assert df2["LegId"].to_list() == [1, 2]


def test_read_yaml_safe(tmp_path):
    """Test that YAML config reading works as expected."""
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text("mode: monte_carlo\nn_runs: 3\n")

    config = FileReader.read_yaml(str(yaml_path))
    assert config["mode"] == "monte_carlo"
    assert config["n_runs"] == 3


@pytest.fixture
def clean_local_storage(tmp_path, monkeypatch):
    """Fixture providing a clean LocalDiskAdapter in a temporary directory."""
    adapter = LocalDiskAdapter()  # no args constructor
    monkeypatch.setenv("LOCAL_DATA_DIR", str(tmp_path))
    # ensure working directory is isolated
    old_cwd = os.getcwd()
    os.chdir(tmp_path)
    yield adapter
    os.chdir(old_cwd)
