import argparse
from dataclasses import dataclass, field, fields as dataclass_fields
from typing import Any, Dict, Optional


@dataclass
class DelayDistribution:
    """
    Represents a simple normal distribution used to generate random delays.
    """
    mean: float = 10.0
    std: float = 3.0


@dataclass
class Config:
    """
    Configuration model for running the flight delay simulation.

    Can be built either from:
      - A YAML/JSON dictionary (FastAPI or config.yaml file)
      - CLI arguments
    """

    #  Parameters needed for the simulation from CLI or form the fast api

    mode: str = "deterministic"       # "deterministic" or "monte_carlo"
    n_runs: int = 3                   # number of Monte Carlo iterations
    min_turnaround: int = 45          # minimum turnaround time (in minutes)
    aircraft_id: Optional[str] = None # optional filter for single aircraft ,for exampled passed from the cli

    # delay distributions
    departure_delay: DelayDistribution = field(default_factory=lambda: DelayDistribution(mean=10, std=3))
    inflight_delay: DelayDistribution = field(default_factory=lambda: DelayDistribution(mean=5, std=2))

    # i/o  settings
    input_schedule: str = "input/schedule 2.csv"
    output_dir: str = "results"
    aggregated_output: str = "aggregated.csv"
    per_run_prefix: str = "run_"

    # For visualization,but did not tested it as it was not part of the requirement.Just as a placeholder there.
    plot: bool = True
    bins: int = 20

    # Storage backend :S3 or local disk . Have not tested this code for S3. But Configurations are there to make the code base extendable.
    storage_backend: str = "local"

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
        cli_args: Optional[argparse.Namespace] = None
    ) -> "Config":
        """
        Creates a Config object from YAML or JSON payload (fasta api).
        If CLI args are passed, their values override the dictionary ones.

        Handles both formats:
          delays:
            departure: {mean, std}
            inflight: {mean, std}
        or
          departure_delay: {mean, std}
          inflight_delay: {mean, std}
        """
        cfg_data = data.copy()

        # Handle nested 'delays' key if present
        delays = cfg_data.pop("delays", {})
        if isinstance(delays, dict):
            if "departure" in delays:
                cfg_data["departure_delay"] = DelayDistribution(**delays["departure"])
            if "inflight" in delays:
                cfg_data["inflight_delay"] = DelayDistribution(**delays["inflight"])

        # Handle flat definitions from the config.yaml file.
        if isinstance(cfg_data.get("departure_delay"), dict):
            cfg_data["departure_delay"] = DelayDistribution(**cfg_data["departure_delay"])
        if isinstance(cfg_data.get("inflight_delay"), dict):
            cfg_data["inflight_delay"] = DelayDistribution(**cfg_data["inflight_delay"])

        # apply CLI overrides if provided from the cli.
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

        # Keep only valid dataclass fields
        valid_fields = {f.name for f in dataclass_fields(cls)}
        filtered = {k: v for k, v in cfg_data.items() if k in valid_fields}

        return cls(**filtered)
