from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from pydantic import BaseModel, Field
from typing import Optional
from pathlib import Path
import os
import shutil
import yaml
import uuid

from flightrobustness.core.models import Config, DelayDistribution
from flightrobustness.core.simulator import run_simulations
from flightrobustness.io.storage_adapters import (
    STORAGE_BACKEND,
)
from flightrobustness.utils.config_loader import load_and_merge_config


app = FastAPI(title="Flight Robustness Simulator", version="1.0")

# Base directory for API uploads
PROJECT_ROOT = Path(__file__).resolve().parents[3]
# user uploaded config.yaml or input files will be stored here
API_DATA_DIR = PROJECT_ROOT / "data/api/uploads"
RESULTS_DIR = PROJECT_ROOT / "data/results"

API_DATA_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


class SimulationRunRequest(BaseModel):
    """Request model for JSON-based simulation runs."""
    mode: str = "monte_carlo"
    n_runs: int = 3
    min_turnaround: int = 45
    input_schedule: Optional[str] = None
    output_dir: str = "data/results"
    aircraft_id: Optional[str] = None
    departure_delay: DelayDistribution = Field(default_factory=lambda: DelayDistribution(10, 3))
    inflight_delay: DelayDistribution = Field(default_factory=lambda: DelayDistribution(5, 2))
    plot: bool = True


class SimulationRunResponse(BaseModel):
    """Response model for completed simulations."""
    message: str
    combined_results_path: str
    aggregated_results_path: str # Tells user where the simulation results can be found
    storage_backend: str = STORAGE_BACKEND # local or S3- useful if we deploy the solution on cloud (aws)

# Just a friendly UI when user lands on the api endpoint
@app.get("/")
def root_service():
    """
    Root endpoint — returns basic service info and available routes.
    """
    return {
        "service": "Flight Robustness Simulator API",
        "version": "1.0",
        "description": (
            "This API simulates cascading flight delays using Monte Carlo or deterministic modes. "
            "Use /docs for full Swagger UI or /api/v1/simulate to run a simulation."
        ),
        "endpoints": {
            "health": "/health",
            "default_config": "/api/v1/config",
            "simulate": "/api/v1/simulate (POST)",
            "cleanup_uploads": "/api/v1/uploads/cleanup",
        },
        "status": "Running"
    }


@app.get("/health")
def health_check():
    """Only for health check,if backend is running"""
    return {"status": "ok", "backend": STORAGE_BACKEND}


@app.get("/api/v1/config")
def get_default_config():
    """Returns default YAML config template.Useful if user needs to know the structure of the yaml.config"""
    return {
        "mode": "monte_carlo",
        "n_runs": 3,
        "min_turnaround": 45,
        "departure_delay": {"mean": 10, "std": 3},
        "inflight_delay": {"mean": 5, "std": 2},
        "plot": True,
    }

# main service for running the simulation
@app.post("/api/v1/simulate", response_model=SimulationRunResponse)
async def run_simulation_api(
    csv_file: UploadFile = File(..., description="Flight schedule CSV file (mandatory)"),
    config_file: Optional[UploadFile] = File(None, description="Optional YAML configuration file"),
    mode: Optional[str] = Form("monte_carlo"),
    n_runs: Optional[int] = Form(3),
    min_turnaround: Optional[int] = Form(45),
    aircraft_id: Optional[str] = Form(None),
    plot: Optional[bool] = Form(True),
):
    """
    Run simulation using:
      - mandatory flight schedule CSV file
      - optional config.yaml file
      - or form fields overriding defaults
    """
    cfg_dict = {}

    # Save uploaded config.yaml . We use data/api/uploads dir.
    if config_file:
        cfg_path = API_DATA_DIR / f"{uuid.uuid4()}_{config_file.filename}"
        with open(cfg_path, "wb") as f:
            shutil.copyfileobj(config_file.file, f)
        with open(cfg_path, "r") as f:
            cfg_dict = yaml.safe_load(f) or {}

    # 2Save uploaded CSV -always required
    csv_path = API_DATA_DIR / f"{uuid.uuid4()}_{csv_file.filename}"
    with open(csv_path, "wb") as f:
        shutil.copyfileobj(csv_file.file, f)
    cfg_dict["input_schedule"] = str(csv_path)

    # like cli these options are configurable
    cfg_dict.update({
        "mode": mode,
        "n_runs": n_runs,
        "min_turnaround": min_turnaround,
        "plot": plot,
        "aircraft_id": aircraft_id,
        "output_dir": str(RESULTS_DIR)
    })

    cleaned = {k: v for k, v in cfg_dict.items() if v not in (None, "", {})}

    # Runs the main simulation
    try:
        cfg = load_and_merge_config(cleaned)
        combined, aggregated = run_simulations(cfg)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Simulation failed: {e}")

    # User uploaded config.yaml are not needed .Just a Cleanup.
    if config_file:
        try:
            os.remove(cfg_path)
        except Exception:
            pass

    #After running the simulations returns the path ,where to  find the csv files.
    combined_path = str(RESULTS_DIR / "modified_input_with_ActualTimeOfArrival.csv")
    aggregated_path = str(RESULTS_DIR / cfg.aggregated_output)

    return SimulationRunResponse(
        message="Simulation completed successfully",
        combined_results_path=combined_path,
        aggregated_results_path=aggregated_path,
    )


@app.delete("/api/v1/uploads/cleanup")
def cleanup_uploads():
    """Deletes temporary uploaded files under data/api/uploads."""
    deleted = []
    for f in API_DATA_DIR.glob("*"):
        try:
            f.unlink()
            deleted.append(f.name)
        except Exception:
            continue
    return {"deleted_files": deleted, "directory": str(API_DATA_DIR)}
