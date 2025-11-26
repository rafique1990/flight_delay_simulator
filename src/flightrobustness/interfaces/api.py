from fastapi import FastAPI, File, UploadFile, Form, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from pathlib import Path
import os
import shutil
import yaml
import uuid
import aiofiles
import aiofiles.os

from flightrobustness.core.models import Config, DelayDistribution
from flightrobustness.core.simulator import run_simulations
from flightrobustness.io.storage_adapters import (
    STORAGE_BACKEND,
)
from flightrobustness.utils.config_loader import load_and_merge_config
from flightrobustness.utils.logger import setup_logger

logger = setup_logger()


app = FastAPI(title="Flight Robustness Simulator", version="1.0")

# Base uploads directory
PROJECT_ROOT = Path(__file__).resolve().parents[3]
API_DATA_DIR = PROJECT_ROOT / "data/api/uploads"
RESULTS_DIR = PROJECT_ROOT / "data/results"

API_DATA_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


class SimulationRunRequest(BaseModel):
    """JSON request payload for simulation runs."""
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
    """Simulation completion response with result paths."""
    message: str
    combined_results_path: str
    aggregated_results_path: str
    storage_backend: str = STORAGE_BACKEND

@app.get("/")
def root_service():
    """Service metadata and available endpoint overview."""
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
    """Health check endpoint."""
    return {"status": "ok", "backend": STORAGE_BACKEND}


@app.get("/api/v1/config")
def get_default_config():
    """Return default configuration template."""
    return {
        "mode": "monte_carlo",
        "n_runs": 3,
        "min_turnaround": 45,
        "departure_delay": {"mean": 10, "std": 3},
        "inflight_delay": {"mean": 5, "std": 2},
        "plot": True,
    }

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
    """Execute simulation with uploaded schedule and optional configuration."""
    cfg_dict = {}

    if config_file:
        cfg_path = API_DATA_DIR / f"{uuid.uuid4()}_{config_file.filename}"
        async with aiofiles.open(cfg_path, "wb") as f:
            while content := await config_file.read(1024 * 1024):
                await f.write(content)
        
        async with aiofiles.open(cfg_path, "r") as f:
            content = await f.read()
            # yaml.safe_load is CPU bound/blocking, run in threadpool
            cfg_dict = await run_in_threadpool(yaml.safe_load, content) or {}

    csv_path = API_DATA_DIR / f"{uuid.uuid4()}_{csv_file.filename}"
    async with aiofiles.open(csv_path, "wb") as f:
        while content := await csv_file.read(1024 * 1024):
            await f.write(content)
            
    cfg_dict["input_schedule"] = str(csv_path)
    cfg_dict.update({
        "mode": mode,
        "n_runs": n_runs,
        "min_turnaround": min_turnaround,
        "plot": plot,
        "aircraft_id": aircraft_id,
        "output_dir": str(RESULTS_DIR)
    })

    cleaned = {k: v for k, v in cfg_dict.items() if v not in (None, "", {})}

    # Run simulation in a separate thread to avoid blocking the event loop
    try:
        cfg = await run_in_threadpool(load_and_merge_config, cleaned)
        combined_df, aggregated_df = await run_in_threadpool(run_simulations, cfg)
    except Exception as e:
        logger.error(f"Simulation failed: {e}")
        raise HTTPException(status_code=500, detail=f"Simulation failed: {e}")

    if config_file:
        try:
            await aiofiles.os.remove(cfg_path)
        except Exception:
            pass

    combined_path = str(RESULTS_DIR / "modified_input_with_ActualTimeOfArrival.csv")
    aggregated_path = str(RESULTS_DIR / cfg.aggregated_output)

    return SimulationRunResponse(
        message="Simulation completed successfully",
        combined_results_path=combined_path,
        aggregated_results_path=aggregated_path,
    )


@app.delete("/api/v1/uploads/cleanup")
async def cleanup_uploads():
    """Remove temporary upload files."""
    deleted = []
    # glob is blocking, but usually fast. For strict async, we could run in threadpool.
    # However, iterating and unlinking is better done carefully.
    # Let's use run_in_threadpool for the glob and iteration if we want to be super safe,
    # or just use aiofiles.os.remove for each file.
    # Since glob returns an iterator, we can't easily await it.
    # We'll collect files first (sync) then remove async.
    
    files = list(API_DATA_DIR.glob("*"))
    
    for f in files:
        try:
            await aiofiles.os.remove(f)
            deleted.append(f.name)
        except Exception:
            continue
    return {"deleted_files": deleted, "directory": str(API_DATA_DIR)}

