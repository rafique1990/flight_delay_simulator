import logging
import pytest
import yaml
from flightrobustness.utils.config_loader import load_and_merge_config
from flightrobustness.utils.logger import setup_logger
from flightrobustness.core.models import Config, DelayDistribution

def test_load_configuration_from_yaml(tmp_path):
    """Config YAML should load into Config dataclass properly."""
    yaml_path = tmp_path / "config.yaml"
    yaml_content = {
        "mode": "deterministic",
        "n_runs": 1,
        "min_turnaround": 45,
        "delays": {
            "departure": {"mean": 12, "std": 4},
            "inflight": {"mean": 8, "std": 2},
        },
    }
    yaml_path.write_text(yaml.dump(yaml_content))

    cfg = load_and_merge_config(str(yaml_path))

    assert isinstance(cfg, Config)
    assert cfg.mode == "deterministic"
    assert isinstance(cfg.departure_delay, DelayDistribution)
    assert cfg.departure_delay.mean == 12
    assert cfg.inflight_delay.std == 2


def test_load_configuration_from_dict_merges_delays():
    """Dict input with nested delays should be converted to DelayDistribution objects."""
    cfg_dict = {
        "mode": "monte_carlo",
        "n_runs": 5,
        "delays": {
            "departure": {"mean": 10, "std": 3},
            "inflight": {"mean": 7, "std": 2},
        },
    }

    cfg = load_and_merge_config(cfg_dict)
    assert isinstance(cfg.departure_delay, DelayDistribution)
    assert cfg.departure_delay.mean == 10
    assert cfg.inflight_delay.mean == 7


def test_load_configuration_invalid_source():
    """Invalid type (non-dict/non-str) should raise ValueError."""
    with pytest.raises(ValueError):
        load_and_merge_config(12345)

def test_logger_setup_creates_logger():
    """setup_logger should create or return a valid logger with correct level."""
    logger = setup_logger("DEBUG")
    assert isinstance(logger, logging.Logger)
    assert logger.level == logging.DEBUG


def test_logger_is_singleton_behavior():
    """setup_logger should not duplicate handlers on multiple calls."""
    logger1 = setup_logger("INFO")
    initial_handlers = len(logger1.handlers)

    logger2 = setup_logger("INFO")
    # same logger, no duplicate handlers
    assert logger1 is logger2
    assert len(logger2.handlers) == initial_handlers


def test_logger_honors_case_insensitivity():
    """setup_logger should handle lowercase level names."""
    logger = setup_logger("warning")
    assert logger.level == logging.WARNING
