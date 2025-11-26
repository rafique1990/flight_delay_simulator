import polars as pl
import numpy as np
import concurrent.futures
import multiprocessing
from typing import List, Dict, Any, Optional, Tuple, Union
from pathlib import Path

from flightrobustness.core.models import Config
from flightrobustness.core.interfaces import DelayGeneratorStrategy
from flightrobustness.core.strategies import DeterministicDelayGenerator, MonteCarloDelayGenerator
from flightrobustness.core.factories import DelayStrategyFactory
from flightrobustness.core.exceptions import SimulationError, DataSourceError
from flightrobustness.io.file_reader import FileReader
from flightrobustness.io.file_writer import FileWriter
from flightrobustness.io.visualizer import plot_arrival_delay_distribution
from flightrobustness.utils.logger import setup_logger

logger = setup_logger()

class FlightDelaySimulator:
    """
    Core simulator class responsible for executing flight delay simulations.
    Uses vectorized operations for high performance on large datasets.
    """
    
    def __init__(self, strategy: DelayGeneratorStrategy, config: Config):
        self.strategy = strategy
        self.config = config

    def run(self, schedule: pl.DataFrame, run_id: int = 0) -> pl.DataFrame:
        """
        Executes a single simulation run using vectorized operations.
        
        Args:
            schedule: The flight schedule DataFrame.
            run_id: The identifier for this simulation run.
            
        Returns:
            DataFrame with simulated delays.
        """
        # Set seed for reproducibility in Monte Carlo
        if isinstance(self.strategy, MonteCarloDelayGenerator):
            np.random.seed(run_id + 42)

        # 1. Generate delays for all flights at once (Vectorized)
        n_rows = schedule.height
        
        # Generate raw delays
        dep_delays = self.strategy.generate_departure_delay(self.config.departure_delay, size=n_rows)
        inflight_delays = self.strategy.generate_inflight_delay(self.config.inflight_delay, size=n_rows)
        
        # Ensure we have a sequence for Polars Series
        if np.isscalar(dep_delays):
            dep_delays = [dep_delays] # type: ignore
        if np.isscalar(inflight_delays):
            inflight_delays = [inflight_delays] # type: ignore
        
        # Add generated delays to DataFrame
        # We need to ensure the order matches the sorting we will apply for accumulation
        # So first, let's sort the schedule to ensure we process legs in order
        sorted_df = schedule.sort(["AircraftId", "STD"])
        
        # Add the generated delays as columns
        df_with_delays = sorted_df.with_columns([
            pl.Series("GeneratedDepDelay", dep_delays),
            pl.Series("GeneratedInflightDelay", inflight_delays)
        ])
        
        # 2. Propagate Delays (Vectorized using Cumulative Sum)
        # Logic:
        # ArrDelay[i] = DepDelay[i] + InflightDelay[i]
        # DepDelay[i] = ArrDelay[i-1] (if same aircraft) + GeneratedDepDelay[i]
        # Therefore:
        # ArrDelay[i] = ArrDelay[i-1] + GeneratedDepDelay[i] + GeneratedInflightDelay[i]
        # This means ArrDelay is the cumulative sum of (GenDep + GenInflight) per aircraft.
        
        result_df = df_with_delays.with_columns([
            (pl.col("GeneratedDepDelay") + pl.col("GeneratedInflightDelay")).alias("TotalAddedDelay")
        ])
        
        result_df = result_df.with_columns([
            pl.col("TotalAddedDelay").cum_sum().over("AircraftId").alias("ArrivalDelay")
        ])
        
        result_df = result_df.with_columns([
            (pl.col("ArrivalDelay") - pl.col("GeneratedInflightDelay")).alias("DepartureDelay"),
            pl.col("GeneratedInflightDelay").alias("InflightDelay"), # Rename for output consistency
            (pl.col("STA") + pl.col("ArrivalDelay")).alias("ActualTimeOfArrival")
        ])
        
        # Rounding for clean output
        result_df = result_df.with_columns([
            pl.col("DepartureDelay").round(2),
            pl.col("InflightDelay").round(2),
            pl.col("ArrivalDelay").round(2),
            pl.col("ActualTimeOfArrival").round(2)
        ])
        
        # Drop intermediate columns
        return result_df.drop(["GeneratedDepDelay", "GeneratedInflightDelay", "TotalAddedDelay"])

def _run_single_process(args: Tuple[str, Config, int]) -> pl.DataFrame:
    """Helper function for ProcessPoolExecutor to run a single simulation."""
    input_path, config, run_id = args
    
    try:
        # Load and filter data inside the worker to avoid pickling large DataFrames
        reader = FileReader()
        df = reader.read_csv(input_path, use_polars=True)
        if config.aircraft_id:
            df = df.filter(pl.col("AircraftId") == config.aircraft_id)
            
        strategy = DelayStrategyFactory.create(config.mode)
        simulator = FlightDelaySimulator(strategy, config)
        sim_df = simulator.run(df, run_id)
        return sim_df.with_columns(pl.lit(run_id).alias("RunId"))
    except Exception as e:
        # Wrap exception to ensure it propagates correctly
        raise SimulationError(f"Run {run_id} failed: {e}") from e

class SimulationOrchestrator:
    """
    Orchestrates the simulation process with concurrency support.
    Follows SRP by delegating specific tasks to helper classes.
    """
    
    def __init__(self, config: Config, reader: Optional[FileReader] = None, writer: Optional[FileWriter] = None):
        self.config = config
        self.reader = reader or FileReader()
        self.writer = writer or FileWriter()

    def run(self) -> pl.DataFrame:
        """Run the full simulation pipeline."""
        logger.info(f"Starting simulation in {self.config.mode} mode")
        
        # 1. Validation (Fail fast)
        if not self.config.input_schedule:
             raise DataSourceError("Input schedule path is not provided in configuration.")

        # 2. Run Simulations (Parallelized)
        n_runs = self.config.n_runs if self.config.mode == "monte_carlo" else 1
        results: List[pl.DataFrame] = []
        
        max_workers = min(n_runs, multiprocessing.cpu_count())
        logger.info(f"Running {n_runs} simulations using {max_workers} workers")
        
        try:
            with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(_run_single_process, (self.config.input_schedule, self.config, i + 1))
                    for i in range(n_runs)
                ]
                
                for future in concurrent.futures.as_completed(futures):
                    try:
                        results.append(future.result())
                    except Exception as e:
                        logger.error(f"Simulation run failed: {e}")
                        raise SimulationError(f"Worker process failed: {e}") from e
        except Exception as e:
             raise SimulationError(f"Simulation execution failed: {e}") from e
            
        # 4. Aggregate Results
        combined_df = pl.concat(results)
        
        # 5. Save Results
        self._save_results(combined_df)
        
        # 6. Plot if requested
        if self.config.plot:
            self._generate_plots(combined_df)
            
        logger.info("Simulation pipeline completed successfully")
        return combined_df

    def _save_results(self, df: pl.DataFrame) -> pl.DataFrame:
        out_dir = Path(self.config.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        
        # Save raw results
        self.writer.write_csv(df, str(out_dir / "all_runs.csv"))
        
        # Save aggregated stats
        # Vectorized aggregation is fast in Polars
        aggregated = (
            df.group_by(["LegId", "AircraftId"])
            .agg([
                (pl.col("ActualTimeOfArrival") - pl.col("STA")).mean().alias("AvgArrivalDelay"),
                (pl.col("ActualTimeOfArrival") - pl.col("STA")).std().alias("StdArrivalDelay"),
                pl.col("ActualTimeOfArrival").min().alias("MinArrival"),
                pl.col("ActualTimeOfArrival").max().alias("MaxArrival"),
                pl.col("ActualTimeOfArrival").quantile(0.95).alias("P95Arrival"),
            ])
        )
        self.writer.write_csv(aggregated, str(out_dir / self.config.aggregated_output))
        return aggregated

    def _generate_plots(self, df: pl.DataFrame) -> None:
        out_dir = Path(self.config.output_dir)
        try:
            plot_arrival_delay_distribution(df, out_dir)
        except Exception as e:
            logger.error(f"Plotting failed: {e}")

# Facade functions for backward compatibility
def run_simulations(cfg: Config) -> Tuple[pl.DataFrame, pl.DataFrame]:
    orchestrator = SimulationOrchestrator(cfg)
    combined_df = orchestrator.run()
    # Recalculate aggregated for return (cheap)
    aggregated = (
        combined_df.group_by(["LegId", "AircraftId"])
        .agg([
            (pl.col("ActualTimeOfArrival") - pl.col("STA")).mean().alias("AvgArrivalDelay"),
            (pl.col("ActualTimeOfArrival") - pl.col("STA")).std().alias("StdArrivalDelay"),
            pl.col("ActualTimeOfArrival").min().alias("MinArrival"),
            pl.col("ActualTimeOfArrival").max().alias("MaxArrival"),
            pl.col("ActualTimeOfArrival").quantile(0.95).alias("P95Arrival"),
        ])
    )
    return combined_df, aggregated

def run_single_simulation(df: pl.DataFrame, cfg: Config, run_id: int) -> pl.DataFrame:
    # Helper to maintain test compatibility
    if cfg.aircraft_id:
        df = df.filter(pl.col("AircraftId") == cfg.aircraft_id)
        
    strategy = DelayStrategyFactory.create(cfg.mode)
    simulator = FlightDelaySimulator(strategy, cfg)
    return simulator.run(df, run_id)
