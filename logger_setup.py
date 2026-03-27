"""
CFXPP Logging Configuration
============================
Sets up logging for all modules in the CFXPP runbook.
"""

import logging
import os

import config


def setup_logging(timestamp=None):
    """
    Configure logging with file and console handlers.

    Args:
        timestamp: Optional timestamp string. If None, generates current timestamp.

    Returns:
        str: Path to the log file created.
    """
    if timestamp is None:
        timestamp = config.get_timestamp()

    log_dir = os.path.join(config.LOGS_DIR, timestamp)
    os.makedirs(log_dir, exist_ok=True)

    latest_dir = os.path.join(config.LOGS_DIR, config.LATEST_FOLDER)
    os.makedirs(latest_dir, exist_ok=True)

    log_file = os.path.join(log_dir, f'cfxpp_{timestamp}.log')
    latest_log_file = os.path.join(latest_dir, 'cfxpp_latest.log')

    log_level = logging.DEBUG if config.DEBUG_MODE else getattr(logging, config.LOG_LEVEL, logging.INFO)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers = []

    formatter = logging.Formatter(config.LOG_FORMAT)

    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    latest_handler = logging.FileHandler(latest_log_file, mode='w', encoding='utf-8')
    latest_handler.setLevel(log_level)
    latest_handler.setFormatter(formatter)
    root_logger.addHandler(latest_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    logger = logging.getLogger(__name__)
    logger.info(f'Logging initialized - Level: {config.LOG_LEVEL}')
    logger.info(f'Log file: {log_file}')

    return log_file
