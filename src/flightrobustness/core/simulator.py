import numpy as np
import polars as pl
from pathlib import Path

from flightrobustness.core.models import Config
from flightrobustness.io.file_reader import FileReader
from flightrobustness.io.file_writer import FileWriter
from flightrobustness.io.visualizer import plot_arrival_delay_distribution


def run_single_simulation(df: pl.DataFrame, cfg, run_id: int) -> pl.DataFrame:
    """Run a single simulation (deterministic or Monte Carlo) with traceable delay fields.Takes user defined settings from Config.yaml and the cli arguments """
    np.random.seed(run_id + 42)
    results = []
    min_turn = cfg.min_turnaround

    if cfg.aircraft_id:
        df = df.filter(
            (pl.col("AircraftId") == cfg.aircraft_id)
            & (pl.col("Origin") != pl.col("Destination"))
        )

    for aircraft in df["AircraftId"].unique():
        subset = (
            df.filter(pl.col("AircraftId") == aircraft)
            .filter(pl.col("Origin") != pl.col("Destination"))
            .sort("STD")
        )
        prev_ata = 0
        for row in subset.iter_rows(named=True):
            # Generate delays,uses config settings
            dep_delay = (
                np.random.normal(cfg.departure_delay.mean, cfg.departure_delay.std)
                if cfg.mode == "monte_carlo"
                else cfg.departure_delay.mean
            )
            inflight_delay = (
                np.random.normal(cfg.inflight_delay.mean, cfg.inflight_delay.std)
                if cfg.mode == "monte_carlo"
                else cfg.inflight_delay.mean
            )

            # Compute actual times
            atd = max(row["STD"], prev_ata + min_turn) + dep_delay
            ata = atd + row["Blocktime"] + inflight_delay
            prev_ata = ata

            # Append simulation metadata
            row["Run"] = run_id
            row["DepartureDelay"] = round(dep_delay, 2)
            row["InFlightDelay"] = round(inflight_delay, 2)
            row["ActualTimeOfArrival"] = round(ata, 2)

            results.append(row)

    return pl.DataFrame(results)



def run_simulations(cfg:Config):
    """Execute all simulation runs and create output + aggregation in the data/results dir(in the project root dir)"""
    out_dir = Path(cfg.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = FileReader.read_csv(cfg.input_schedule, use_polars=True)
    runs = (
        [run_single_simulation(df, cfg, 1)]
        if cfg.mode == "deterministic"
        else [run_single_simulation(df, cfg, i + 1) for i in range(cfg.n_runs)]
    )

    combined = pl.concat(runs)
    FileWriter.write_csv(combined, str(out_dir / "modified_input_with_ActualTimeOfArrival.csv"))

    aggregated = (
        combined.groupby(["LegId", "AircraftId"])
        .agg([
            (pl.col("ActualTimeOfArrival") - pl.col("STA")).mean().alias("AvgArrivalDelay"),
            (pl.col("ActualTimeOfArrival") - pl.col("STA")).std().alias("StdArrivalDelay"),
            pl.col("ActualTimeOfArrival").mean().alias("AvgActualTimeOfArrival"),
        ])
        .sort(["AircraftId", "LegId"])
    )
    FileWriter.write_csv(aggregated, str(out_dir / cfg.aggregated_output))

    if cfg.plot:
        plot_arrival_delay_distribution(combined, out_dir, cfg.bins)

    return combined, aggregated
