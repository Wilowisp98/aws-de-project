# -*- coding: utf-8 -*-
import logging

def setup_logging(log_level: str = "INFO"):
    """
    Configures the root logger with the log level from application config,
    sets up a standardized format for log messages with timestamps, and
    returns a logger instance for the current module.
    
    Returns:
        logging.Logger: Logger instance for the current module (__name__).
    """
    level = log_level
    
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    return logging.getLogger(__name__)