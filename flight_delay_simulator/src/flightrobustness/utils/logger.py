import logging

##TODO: Logging through the application and may be use some monitoring tool like prometheus and grafana
def setup_logger(level: str = 'INFO') -> logging.Logger:
    """
    Sets up a basic logger for the application based on a configuration level string.
    """
    logger = logging.getLogger('flightrobustness')
    # Use upper() to ensure compatibility with logging levels (INFO, DEBUG, etc.)
    logger.setLevel(level.upper())

    # Check if handler already exists to prevent duplicate messages
    if not logger.handlers:
        ch = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    # Prevent logging messages from bubbling up to the root logger if desired
    logger.propagate = False

    return logger
