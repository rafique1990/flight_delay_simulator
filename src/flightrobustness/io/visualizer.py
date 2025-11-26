import matplotlib.pyplot as plt
from pathlib import Path
import polars as pl

def plot_arrival_delay_distribution(df: pl.DataFrame, output_dir: Path, bins: int = 20):
    """Generate histogram of actual arrival times."""
    try:
        plt.figure(figsize=(8, 5))
        df["ActualTimeOfArrival"].to_pandas().hist(bins=bins)
        plt.title("Distribution of Actual Arrival Times")
        plt.xlabel("Minutes")
        plt.ylabel("Number of Flights")
        plt.tight_layout()
        path = output_dir / "arrival_delay_distribution.png"
        plt.savefig(path)
        plt.close()
    except Exception as e:
        print(f"Plotting failed: {e}")
