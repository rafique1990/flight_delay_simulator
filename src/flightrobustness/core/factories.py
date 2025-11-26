from typing import Type
from flightrobustness.core.interfaces import DelayGeneratorStrategy
from flightrobustness.core.strategies import DeterministicDelayGenerator, MonteCarloDelayGenerator
from flightrobustness.core.exceptions import ConfigurationError

class DelayStrategyFactory:
    """Factory for creating delay generator strategies."""
    
    _strategies = {
        "deterministic": DeterministicDelayGenerator,
        "monte_carlo": MonteCarloDelayGenerator
    }
    
    @classmethod
    def create(cls, mode: str) -> DelayGeneratorStrategy:
        """
        Create a delay generator strategy based on the mode.
        
        Args:
            mode: The simulation mode ('deterministic' or 'monte_carlo').
            
        Returns:
            An instance of a DelayGeneratorStrategy.
            
        Raises:
            ConfigurationError: If the mode is unknown.
        """
        strategy_class = cls._strategies.get(mode.lower())
        if not strategy_class:
            raise ConfigurationError(f"Unknown simulation mode: {mode}. Available: {list(cls._strategies.keys())}")
        return strategy_class()
