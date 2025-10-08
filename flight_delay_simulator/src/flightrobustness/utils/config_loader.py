import yaml
from typing import Union, Dict, Any
from flightrobustness.core.models import Config, DelayDistribution


def load_and_merge_config(data_or_path: Union[str, Dict[str, Any]]) -> Config:
    """Accepts a dict (from API) or YAML path, returns Config."""
    # --- Load from YAML or dict ---
    if isinstance(data_or_path, str):
        with open(data_or_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    elif isinstance(data_or_path, dict):
        data = data_or_path.copy()
    else:
        raise ValueError("Config source must be dict or YAML path")

    # handles the backward compatibility for nested 'delays'
    if "delays" in data:
        delays = data.pop("delays")
        if isinstance(delays, dict):
            if "departure" in delays:
                data["departure_delay"] = delays["departure"]
            if "inflight" in delays:
                data["inflight_delay"] = delays["inflight"]

    #converts the nested dicts to DelayDistribution dataclasses
    if isinstance(data.get("departure_delay"), dict):
        data["departure_delay"] = DelayDistribution(**data["departure_delay"])
    if isinstance(data.get("inflight_delay"), dict):
        data["inflight_delay"] = DelayDistribution(**data["inflight_delay"])
    return Config(**data)
