import polars as pl
from flightrobustness.core.simulator import run_single_simulation, run_simulations


def test_run_single_simulation_deterministic(create_dummy_schedule_csv):
    """Ensure deterministic run creates reproducible ATA."""
    cfg = create_dummy_schedule_csv
    df = pl.read_csv(cfg.input_schedule)

    result = run_single_simulation(df, cfg, 1)
    assert "ActualTimeOfArrival" in result.columns
    assert result.height == df.height


def test_run_simulations_monte_carlo(create_dummy_schedule_csv):
    """Ensure Monte Carlo mode executes multiple runs and aggregates correctly."""
    cfg = create_dummy_schedule_csv
    cfg.mode = "monte_carlo"
    combined, aggregated = run_simulations(cfg)

    assert not combined.is_empty()
    assert not aggregated.is_empty()
    assert "AvgArrivalDelay" in aggregated.columns
    assert combined.height > aggregated.height


def test_no_aircraft_filter_leaks(create_dummy_schedule_csv):
    """Ensure aircraft filter doesn’t drop data accidentally."""
    cfg = create_dummy_schedule_csv
    df = pl.read_csv(cfg.input_schedule)
    result = run_single_simulation(df, cfg, 1)

    # All aircraft IDs from input must appear in output
    assert set(df["AircraftId"]) == set(result["AircraftId"])
