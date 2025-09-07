import logging
from typing import Optional


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Retrieve a configured logger for the specified module.
    
    If no name is provided, the default logger name 'GridMR' is used.
    Each logger is configured with:
    - A StreamHandler for console output
    - A standardized log format with timestamp, level, logger name, and message
    
    Args:
        name (Optional[str]): Name of the logger (usually the module name).
    
    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name or "GridMR")
    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '[%(asctime)s] %(levelname)s %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def setup_logger():
    """
    Initialize and configure the global logger for the GridMR application.
    
    This function ensures a consistent logging setup across the system.
    It is typically called during application startup.
    
    Returns:
        logging.Logger: Global application logger.
    """
    logger = get_logger()
    logger.info("GridMR logger initialized")
    return logger