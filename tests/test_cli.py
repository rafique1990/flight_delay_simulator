import subprocess
import sys
import os
import pytest
from unittest.mock import patch, MagicMock
from flightrobustness.interfaces.cli import main

def test_cli_runs_with_minimal_config(tmp_path):
    """Run simulate-cli using a temporary YAML config + dummy CSV file."""
    # Create dummy CSV input
    input_dir = tmp_path / "data" / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    csv_path = input_dir / "schedule 2.csv"
    csv_path.write_text("LegId,STD,STA,AircraftId,Origin,Destination,Blocktime\n1,600,700,AC1,FRA,MUC,100\n")

    # Write YAML config pointing to that CSV
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        f"mode: deterministic\nn_runs: 1\nmin_turnaround: 45\ninput_schedule: '{csv_path}'\noutput_dir: '{tmp_path}/results'\n"
    )

    # Run CLI
    env = sys.path.copy()
    # Ensure src is in pythonpath
    env_vars = {**os.environ, "PYTHONPATH": str(tmp_path.parent.parent / "src") + os.pathsep + os.environ.get("PYTHONPATH", "")}
    
    # Actually, simpler to just use the current sys.path
    env_vars = os.environ.copy()
    env_vars["PYTHONPATH"] = os.pathsep.join(sys.path)

    result = subprocess.run(
        [sys.executable, "-m", "flightrobustness.interfaces.cli", "--config", str(config_file)],
        capture_output=True,
        text=True,
        env=env_vars
    )

    assert result.returncode == 0
    assert "Running simulation" in result.stdout
    assert "Simulation complete" in result.stdout


def test_cli_main_function_direct_call(tmp_path):
    """Test calling main() directly with mocked args to verify overrides."""
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    csv_path = input_dir / "test.csv"
    csv_path.write_text("LegId,STD,STA,AircraftId,Origin,Destination,Blocktime\n1,600,700,AC1,FRA,MUC,100\n")
    
    config_file = tmp_path / "config.yaml"
    config_file.write_text(f"input_schedule: '{csv_path}'\noutput_dir: '{tmp_path}/results'")
    
    test_args = [
        "simulate-cli",
        "--config", str(config_file),
        "--mode", "monte_carlo",
        "--runs", "2",
        "--aircraftid", "AC1"
    ]
    
    with patch.object(sys, 'argv', test_args):
        # We need to mock run_simulations to avoid actual execution if we want just to test parsing,
        # but running it is also fine for integration test.
        # Let's mock it to inspect the config passed to it.
        with patch("flightrobustness.interfaces.cli.run_simulations") as mock_run:
            mock_run.return_value = (MagicMock(), MagicMock())
            main()
            
            # Verify config was updated correctly
            args, _ = mock_run.call_args
            cfg = args[0]
            assert cfg.mode == "monte_carlo"
            assert cfg.n_runs == 2
            assert cfg.aircraft_id == "AC1"


def test_cli_config_load_failure():
    """Test CLI exits if config load fails."""
    with patch.object(sys, 'argv', ["simulate-cli", "--config", "non_existent.yaml"]):
        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 1


def test_cli_simulation_failure(tmp_path):
    """Test CLI exits if simulation fails."""
    # Create a valid config file to pass the first check
    config_file = tmp_path / "config.yaml"
    config_file.write_text("mode: deterministic")
    
    with patch.object(sys, 'argv', ["simulate-cli", "--config", str(config_file)]):
        with patch("flightrobustness.interfaces.cli.run_simulations", side_effect=Exception("Sim Error")):
            with pytest.raises(SystemExit) as exc:
                main()
            assert exc.value.code == 1
