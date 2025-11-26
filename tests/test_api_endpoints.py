import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch
from pathlib import Path
from flightrobustness.interfaces.api import app, API_DATA_DIR, RESULTS_DIR


client = TestClient(app)


def test_root_service():
    """Root endpoint should return API metadata."""
    resp = client.get("/")
    assert resp.status_code == 200
    data = resp.json()
    assert data["service"] == "Flight Robustness Simulator API"
    assert "simulate" in data["endpoints"]
    assert data["status"] == "Running"


def test_health_check():
    """Health check should return status OK."""
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_get_default_config():
    """Config endpoint should return default YAML-like config."""
    resp = client.get("/api/v1/config")
    assert resp.status_code == 200
    data = resp.json()
    assert "mode" in data
    assert "departure_delay" in data
    assert data["mode"] == "monte_carlo"


@pytest.fixture
def dummy_csv_file(tmp_path):
    """Create a simple dummy flight schedule CSV."""
    csv_path = tmp_path / "dummy_schedule.csv"
    csv_path.write_text(
        "LegId,Origin,Destination,AircraftId,STD,STA,Blocktime\n"
        "1,FRA,MUC,AC1,600,700,100\n"
        "2,MUC,FRA,AC1,750,850,100\n"
    )
    return csv_path


@pytest.fixture
def dummy_config_file(tmp_path):
    """Create a simple YAML config."""
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text("mode: monte_carlo\nn_runs: 2\nmin_turnaround: 45\n")
    return yaml_path


def test_run_simulation_with_csv_only(dummy_csv_file):
    """Should process simulation with only a CSV upload."""
    with open(dummy_csv_file, "rb") as f:
        files = {"csv_file": ("dummy_schedule.csv", f, "text/csv")}
        resp = client.post("/api/v1/simulate", files=files)

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert "Simulation completed successfully" in data["message"]
    assert data["combined_results_path"].endswith(".csv")
    assert data["aggregated_results_path"].endswith(".csv")
    assert RESULTS_DIR.exists()


def test_run_simulation_with_csv_and_config(dummy_csv_file, dummy_config_file):
    """Should process simulation with both CSV + config.yaml uploads."""
    with open(dummy_csv_file, "rb") as csvf, open(dummy_config_file, "rb") as ymlf:
        files = {
            "csv_file": ("dummy_schedule.csv", csvf, "text/csv"),
            "config_file": ("config.yaml", ymlf, "application/x-yaml"),
        }
        resp = client.post("/api/v1/simulate", files=files)

    assert resp.status_code == 200
    data = resp.json()
    assert "Simulation completed successfully" in data["message"]
    assert "aggregated_results_path" in data
    assert "combined_results_path" in data


def test_run_simulation_missing_csv():
    """Missing CSV should raise validation error."""
    resp = client.post("/api/v1/simulate")
    assert resp.status_code == 422  # FastAPI validation error (missing csv_file)


def test_cleanup_uploads_creates_and_deletes(tmp_path):
    """Should delete uploaded files under data/api/uploads."""
    # Create a dummy file in API_DATA_DIR
    API_DATA_DIR.mkdir(parents=True, exist_ok=True)
    dummy = API_DATA_DIR / "temp_test.txt"
    dummy.write_text("test123")

    resp = client.delete("/api/v1/uploads/cleanup")
    assert resp.status_code == 200
    data = resp.json()

    assert "deleted_files" in data
    assert "temp_test.txt" in data["deleted_files"]
    assert dummy.exists() is False


def test_run_simulation_api_failure(dummy_csv_file):
    """Test that API returns 500 if simulation fails."""
    with open(dummy_csv_file, "rb") as f:
        files = {"csv_file": ("dummy_schedule.csv", f, "text/csv")}
        
        # Mock run_simulations to raise exception
        with patch("flightrobustness.interfaces.api.run_simulations", side_effect=Exception("Sim Error")):
            resp = client.post("/api/v1/simulate", files=files)
            
    assert resp.status_code == 500
    assert "Simulation failed: Sim Error" in resp.json()["detail"]


def test_cleanup_uploads_partial_failure(tmp_path):
    """Test cleanup handles deletion errors gracefully."""
    API_DATA_DIR.mkdir(parents=True, exist_ok=True)
    dummy = API_DATA_DIR / "locked.txt"
    dummy.write_text("test")
    
    # Mock unlink to raise exception for this file
    with patch("aiofiles.os.remove", side_effect=Exception("Locked")):
        resp = client.delete("/api/v1/uploads/cleanup")
        
    assert resp.status_code == 200
    # Should complete without crashing, but file won't be in deleted list
    data = resp.json()
    assert "locked.txt" not in data["deleted_files"]

