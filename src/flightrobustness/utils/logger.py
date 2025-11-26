import logging

def setup_logger(level: str = 'INFO') -> logging.Logger:
    """Configure application logger with specified level."""
    logger = logging.getLogger('flightrobustness')
    logger.setLevel(level.upper())

    if not logger.handlers:
        ch = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    logger.propagate = False

    return logger
