import numpy as np
from typing import Any, Union
from flightrobustness.core.interfaces import DelayGeneratorStrategy
from flightrobustness.core.models import DelayDistribution

class DeterministicDelayGenerator(DelayGeneratorStrategy):
    """Generates fixed delays equal to the mean of the distribution."""
    
    def generate_departure_delay(self, distribution: DelayDistribution, size: int = 1) -> Union[float, np.ndarray]:
        if size == 1:
            return float(distribution.mean)
        return np.full(size, distribution.mean)

    def generate_inflight_delay(self, distribution: DelayDistribution, size: int = 1) -> Union[float, np.ndarray]:
        if size == 1:
            return float(distribution.mean)
        return np.full(size, distribution.mean)

class MonteCarloDelayGenerator(DelayGeneratorStrategy):
    """Generates random delays based on a normal distribution."""
    
    def generate_departure_delay(self, distribution: DelayDistribution, size: int = 1) -> Union[float, np.ndarray]:
        if size == 1:
            return float(max(0.0, np.random.normal(distribution.mean, distribution.std)))
        # Vectorized generation
        delays = np.random.normal(distribution.mean, distribution.std, size)
        return np.maximum(0.0, delays)

    def generate_inflight_delay(self, distribution: DelayDistribution, size: int = 1) -> Union[float, np.ndarray]:
        if size == 1:
            return float(max(0.0, np.random.normal(distribution.mean, distribution.std)))
        delays = np.random.normal(distribution.mean, distribution.std, size)
        return np.maximum(0.0, delays)
