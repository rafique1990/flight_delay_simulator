import argparse
from dataclasses import dataclass, field, fields as dataclass_fields
from typing import Any, Dict, Optional


@dataclass
class DelayDistribution:
    """Normal distribution parameters for delay generation."""
    mean: float = 10.0
    std: float = 3.0


@dataclass
class Config:
    """Simulation configuration with support for CLI and YAML/JSON sources."""

    mode: str = "deterministic"
    n_runs: int = 3
    min_turnaround: int = 45
    aircraft_id: Optional[str] = None

    departure_delay: DelayDistribution = field(default_factory=lambda: DelayDistribution(mean=10, std=3))
    inflight_delay: DelayDistribution = field(default_factory=lambda: DelayDistribution(mean=5, std=2))

    input_schedule: str = "input/schedule 2.csv"
    output_dir: str = "results"
    aggregated_output: str = "aggregated.csv"
    per_run_prefix: str = "run_"

    plot: bool = True
    bins: int = 20

    storage_backend: str = "local"

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
        cli_args: Optional[argparse.Namespace] = None
    ) -> "Config":
        """
        Build Config from dictionary with optional CLI overrides.
        
        Supports both nested and flat delay configurations.
        """
        cfg_data = data.copy()

        delays = cfg_data.pop("delays", {})
        if isinstance(delays, dict):
            if "departure" in delays:
                cfg_data["departure_delay"] = DelayDistribution(**delays["departure"])
            if "inflight" in delays:
                cfg_data["inflight_delay"] = DelayDistribution(**delays["inflight"])

        if isinstance(cfg_data.get("departure_delay"), dict):
            cfg_data["departure_delay"] = DelayDistribution(**cfg_data["departure_delay"])
        if isinstance(cfg_data.get("inflight_delay"), dict):
            cfg_data["inflight_delay"] = DelayDistribution(**cfg_data["inflight_delay"])

        if cli_args:
            if getattr(cli_args, "mode", None):
                cfg_data["mode"] = cli_args.mode
            if getattr(cli_args, "input", None):
                cfg_data["input_schedule"] = cli_args.input
            if getattr(cli_args, "output", None):
                cfg_data["output_dir"] = cli_args.output
            if getattr(cli_args, "aircraftid", None):
                cfg_data["aircraft_id"] = cli_args.aircraftid
            if getattr(cli_args, "runs", None):
                cfg_data["n_runs"] = int(cli_args.runs)

        valid_fields = {f.name for f in dataclass_fields(cls)}
        filtered = {k: v for k, v in cfg_data.items() if k in valid_fields}

        return cls(**filtered)
