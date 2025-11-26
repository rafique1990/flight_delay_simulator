import argparse
import sys
from flightrobustness.core.models import Config
from flightrobustness.core.simulator import run_simulations
from flightrobustness.io.file_reader import FileReader


def main():
    """Flight delay simulator command-line interface."""
    parser = argparse.ArgumentParser(description="Flight Delay Simulator")
    parser.add_argument("--config", default="config.yaml", help="YAML config path")
    parser.add_argument("--mode", choices=["deterministic", "monte_carlo"])
    parser.add_argument("--input", help="Override input CSV")
    parser.add_argument("--runs", help="Number of simulations to run")
    parser.add_argument("--output", help="Override output directory")
    parser.add_argument("--aircraftid", help="Optional: single-aircraft simulation")
    args = parser.parse_args()

    try:
        reader = FileReader()
        cfg_yaml = reader.read_yaml(args.config)
        cfg = Config.from_dict(cfg_yaml, args)

    except Exception as e:
        print(f"Failed to load configuration: {e}")
        sys.exit(1)

    if args.mode: cfg.mode = args.mode
    if args.input: cfg.input_schedule = args.input
    if args.output: cfg.output_dir = args.output
    if args.aircraftid: cfg.aircraft_id = args.aircraftid
    if args.runs: cfg.n_runs = int(args.runs)

    print(f"\nRunning simulation: mode={cfg.mode}, runs={cfg.n_runs}\n")

    try:
        combined, aggregated = run_simulations(cfg)
        print(f"Simulation complete. Results in: {cfg.output_dir}")
    except Exception as e:
        print(f"Simulation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()