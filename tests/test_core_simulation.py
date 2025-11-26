import polars as pl
from unittest.mock import patch
from flightrobustness.core.simulator import run_single_simulation, run_simulations


def test_run_single_simulation_deterministic(create_dummy_schedule_csv):
    """Test deterministic simulation produces expected results."""
    cfg = create_dummy_schedule_csv
    df = pl.read_csv(cfg.input_schedule)

    result = run_single_simulation(df, cfg, 1)
    assert "ActualTimeOfArrival" in result.columns
    assert result.height == df.height


def test_run_simulations_monte_carlo(create_dummy_schedule_csv):
    """Test Monte Carlo mode executes multiple runs correctly."""
    cfg = create_dummy_schedule_csv
    cfg.mode = "monte_carlo"
    combined, aggregated = run_simulations(cfg)

    assert not combined.is_empty()
    assert not aggregated.is_empty()
    assert "AvgArrivalDelay" in aggregated.columns
    assert combined.height > aggregated.height


def test_no_aircraft_filter_leaks(create_dummy_schedule_csv):
    """Verify aircraft filter preserves all data correctly."""
    cfg = create_dummy_schedule_csv
    df = pl.read_csv(cfg.input_schedule)
    result = run_single_simulation(df, cfg, 1)

    # All aircraft IDs from input must appear in output
    assert set(df["AircraftId"]) == set(result["AircraftId"])


def test_run_simulations_creates_directory_and_plots(create_dummy_schedule_csv, tmp_path):
    """Test that run_simulations creates output directory and calls plotter."""
    cfg = create_dummy_schedule_csv
    # Point output to a new non-existent directory
    output_dir = tmp_path / "new_results"
    cfg.output_dir = str(output_dir)
    cfg.plot = True
    
    # We mock the plotter to verify it's called, avoiding actual matplotlib usage
    with patch("flightrobustness.core.simulator.plot_arrival_delay_distribution") as mock_plot:
        run_simulations(cfg)
        
        assert output_dir.exists()
        assert (output_dir / cfg.aggregated_output).exists()
        mock_plot.assert_called_once()


def test_run_single_simulation_monte_carlo_randomness(create_dummy_schedule_csv):
    """Test that Monte Carlo mode produces different results for different seeds/runs."""
    cfg = create_dummy_schedule_csv
    cfg.mode = "monte_carlo"
    df = pl.read_csv(cfg.input_schedule)
    
    # Run twice with different run_ids
    res1 = run_single_simulation(df, cfg, 1)
    res2 = run_single_simulation(df, cfg, 2)
    
    # Delays should likely be different (unless we are extremely unlucky with random seed)
    # We check if at least one delay is different
    delays1 = res1["DepartureDelay"].to_list()
    delays2 = res2["DepartureDelay"].to_list()
    
    assert delays1 != delays2


def test_run_single_simulation_with_aircraft_filter(create_dummy_schedule_csv):
    """Test that simulation filters by aircraft ID when configured."""
    cfg = create_dummy_schedule_csv
    cfg.aircraft_id = "AC1"
    df = pl.read_csv(cfg.input_schedule)
    
    # Ensure input has multiple aircraft to verify filtering
    # The dummy schedule currently only has AC1. Let's add another one.
    df2 = pl.DataFrame({
        "LegId": [3], "Origin": ["BER"], "Destination": ["LHR"], 
        "AirlineDesignator": ["CLF"], "FlightNbr": ["SRE126"],
        "AircraftId": ["AC2"], "STD": [1200], "STA": [1300], 
        "Blocktime": [60], "Distance": [500.0], "SubfleetType": ["CR9"],
        "StopIdentifier": [6883], "FractionBlock": [492.4],
        "StartPnrRange": ["CUUQ26ZG"], "EligibleBc": [True]
    })
    df_combined = pl.concat([df, df2])
    
    result = run_single_simulation(df_combined, cfg, 1)
    
    assert result.height > 0
    assert all(result["AircraftId"] == "AC1")
    assert "AC2" not in result["AircraftId"]
