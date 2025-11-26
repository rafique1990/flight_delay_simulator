import subprocess
import sys

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
    result = subprocess.run(
        [sys.executable, "-m", "flightrobustness.interfaces.cli", "--config", str(config_file)],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "Running simulation" in result.stdout
    assert "Simulation complete" in result.stdout
