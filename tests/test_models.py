import pytest
import argparse
from flightrobustness.core.models import Config, DelayDistribution


def test_delay_distribution_defaults():
    """Test DelayDistribution default values."""
    dist = DelayDistribution()
    assert dist.mean == 10.0
    assert dist.std == 3.0


def test_delay_distribution_custom_values():
    """Test DelayDistribution with custom values."""
    dist = DelayDistribution(mean=15.0, std=5.0)
    assert dist.mean == 15.0
    assert dist.std == 5.0


def test_config_defaults():
    """Test Config default values."""
    cfg = Config()
    assert cfg.mode == "deterministic"
    assert cfg.n_runs == 3
    assert cfg.min_turnaround == 45
    assert cfg.aircraft_id is None
    assert cfg.storage_backend == "local"
    assert cfg.plot is True


def test_config_custom_values():
    """Test Config with custom values."""
    cfg = Config(
        mode="monte_carlo",
        n_runs=10,
        min_turnaround=60,
        aircraft_id="AC123"
    )
    assert cfg.mode == "monte_carlo"
    assert cfg.n_runs == 10
    assert cfg.min_turnaround == 60
    assert cfg.aircraft_id == "AC123"


def test_config_from_dict_nested_delays():
    """Test Config.from_dict with nested delay structure."""
    data = {
        "mode": "monte_carlo",
        "n_runs": 5,
        "delays": {
            "departure": {"mean": 12, "std": 4},
            "inflight": {"mean": 8, "std": 3}
        }
    }
    cfg = Config.from_dict(data)
    
    assert cfg.mode == "monte_carlo"
    assert cfg.n_runs == 5
    assert isinstance(cfg.departure_delay, DelayDistribution)
    assert cfg.departure_delay.mean == 12
    assert cfg.departure_delay.std == 4
    assert cfg.inflight_delay.mean == 8
    assert cfg.inflight_delay.std == 3


def test_config_from_dict_flat_delays():
    """Test Config.from_dict with flat delay structure."""
    data = {
        "mode": "deterministic",
        "departure_delay": {"mean": 15, "std": 5},
        "inflight_delay": {"mean": 10, "std": 2}
    }
    cfg = Config.from_dict(data)
    
    assert isinstance(cfg.departure_delay, DelayDistribution)
    assert cfg.departure_delay.mean == 15
    assert cfg.inflight_delay.mean == 10


def test_config_from_dict_with_cli_overrides():
    """Test Config.from_dict with CLI argument overrides."""
    data = {
        "mode": "deterministic",
        "n_runs": 1,
        "input_schedule": "input/original.csv"
    }
    
    cli_args = argparse.Namespace(
        mode="monte_carlo",
        input="input/override.csv",
        output="results/override",
        aircraftid="AC999",
        runs="10"
    )
    
    cfg = Config.from_dict(data, cli_args)
    
    assert cfg.mode == "monte_carlo"
    assert cfg.input_schedule == "input/override.csv"
    assert cfg.output_dir == "results/override"
    assert cfg.aircraft_id == "AC999"
    assert cfg.n_runs == 10


def test_config_from_dict_cli_partial_overrides():
    """Test Config.from_dict with partial CLI overrides."""
    data = {
        "mode": "deterministic",
        "n_runs": 1
    }
    
    cli_args = argparse.Namespace(
        mode="monte_carlo",
        input=None,
        output=None,
        aircraftid=None,
        runs=None
    )
    
    cfg = Config.from_dict(data, cli_args)
    assert cfg.mode == "monte_carlo"
    assert cfg.n_runs == 1


def test_config_from_dict_filters_invalid_fields():
    """Test that invalid fields are filtered out."""
    data = {
        "mode": "deterministic",
        "invalid_field": "should_be_ignored",
        "another_invalid": 123
    }
    
    cfg = Config.from_dict(data)
    assert cfg.mode == "deterministic"
    assert not hasattr(cfg, "invalid_field")


def test_config_from_dict_delay_objects_preserved():
    """Test that DelayDistribution objects are preserved if already instantiated."""
    departure = DelayDistribution(mean=20, std=6)
    inflight = DelayDistribution(mean=12, std=4)
    
    data = {
        "mode": "monte_carlo",
        "departure_delay": departure,
        "inflight_delay": inflight
    }
    
    cfg = Config.from_dict(data)
    assert cfg.departure_delay.mean == 20
    assert cfg.inflight_delay.mean == 12
