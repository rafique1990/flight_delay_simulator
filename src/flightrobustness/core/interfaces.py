from abc import ABC, abstractmethod
from typing import List, Protocol, Any
import polars as pl
from flightrobustness.core.models import Config, DelayDistribution

class DelayGeneratorStrategy(ABC):
    """Strategy interface for generating flight delays."""
    
    @abstractmethod
    def generate_departure_delay(self, distribution: DelayDistribution, size: int = 1) -> Any:
        """Generate departure delay(s). Returns float or np.ndarray."""
        pass

    @abstractmethod
    def generate_inflight_delay(self, distribution: DelayDistribution, size: int = 1) -> Any:
        """Generate in-flight delay(s). Returns float or np.ndarray."""
        pass

class ScheduleRepository(Protocol):
    """Interface for reading flight schedules."""
    
    def load_schedule(self, source: str) -> pl.DataFrame:
        ...

class ResultRepository(Protocol):
    """Interface for saving simulation results."""
    
    def save_results(self, df: pl.DataFrame, path: str) -> str:
        ...
