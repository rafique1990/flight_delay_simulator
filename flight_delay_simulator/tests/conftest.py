import sys
import pytest
import polars as pl
from pathlib import Path
from fastapi.testclient import TestClient

###TODO: Store the test files and test results in appropraite directories at the moment ,there is issue where the test results are stored.

# Ensure src/ is importable
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from flightrobustness.core.models import Config, DelayDistribution
from flightrobustness.interfaces.api import app as fastapi_app


@pytest.fixture(scope="session")
def test_client():
    """Provides a FastAPI TestClient for integration testing."""
    return TestClient(fastapi_app)


@pytest.fixture
def config_with_tmp_storage(tmp_path, monkeypatch) -> Config:
    """Temporary configuration for isolated test runs."""
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("LOCAL_DATA_DIR", str(tmp_path))

    results_dir = tmp_path / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    cfg = Config(
        mode="monte_carlo",
        n_runs=3,
        min_turnaround=45,
        departure_delay=DelayDistribution(mean=10, std=3),
        inflight_delay=DelayDistribution(mean=5, std=2),
        storage_backend="local",
        output_dir=str(results_dir),
        plot=False,
    )
    return cfg


@pytest.fixture
def isolated_env(tmp_path, monkeypatch):
    """Provides isolated environment for I/O tests."""
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("LOCAL_DATA_DIR", str(tmp_path))
    out_dir = tmp_path / "results"
    out_dir.mkdir(parents=True, exist_ok=True)
    return str(out_dir)


@pytest.fixture
def create_dummy_schedule_csv(tmp_path):
    """Creates a small dummy schedule CSV file and returns a Config pointing to it."""
    input_dir = tmp_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    csv_path = input_dir / "dummy_schedule.csv"

    # Write a tiny schedule CSV
    df = pl.DataFrame({
        "LegId": [0, 1, 2],
        "Origin": ["FRA", "MUC", "FRA"],
        "Destination": ["MUC", "FRA", "BER"],
        "AirlineDesignator": ["CLF", "CLF", "CLF"],
        "FlightNbr": ["SRE123", "SRE124", "SRE125"],
        "AircraftId": ["AC1", "AC1", "AC1"],
        "STD": [600, 750, 950],
        "STA": [700, 850, 1050],
        "Blocktime": [100, 100, 90],
        "Distance": [350.0, 350.0, 300.0],
        "SubfleetType": ["CR9", "CR9", "CR9"],
        "StopIdentifier": [6883, 6883, 6883],
        "FractionBlock": [492.4, 492.4, 492.4],
        "StartPnrRange": ["CUUQ26ZG", "CUUQ26ZG", "CUUQ26ZG"],
        "EligibleBc": [True, True, True],
    })
    df.write_csv(csv_path)

    # Return a valid Config with correct absolute path
    from flightrobustness.core.models import Config, DelayDistribution
    cfg = Config(
        mode="monte_carlo",
        n_runs=3,
        min_turnaround=45,
        departure_delay=DelayDistribution(mean=10, std=3),
        inflight_delay=DelayDistribution(mean=5, std=2),
        input_schedule=str(csv_path),  # ✅ absolute path
        output_dir=str(tmp_path / "results"),
    )

    return cfg


@pytest.fixture
def clean_local_storage(tmp_path, monkeypatch):
    """Fixture to provide a clean LocalDiskAdapter with temp directory override."""
    from flightrobustness.io.storage_adapters import LocalDiskAdapter

    adapter = LocalDiskAdapter()

    # Override its data directory (LOCAL_DATA_DIR env or internal attr)
    monkeypatch.setenv("LOCAL_DATA_DIR", str(tmp_path))
    adapter.LOCAL_DATA_DIR = str(tmp_path)

    yield adapter

    for f in tmp_path.glob("*"):
        f.unlink(missing_ok=True)
