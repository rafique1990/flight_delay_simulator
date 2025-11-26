class FlightRobustnessError(Exception):
    """Base exception for the flight robustness simulator."""
    pass

class SimulationError(FlightRobustnessError):
    """Raised when a simulation run fails."""
    pass

class ConfigurationError(FlightRobustnessError):
    """Raised when configuration is invalid."""
    pass

class DataSourceError(FlightRobustnessError):
    """Raised when data loading fails."""
    pass
