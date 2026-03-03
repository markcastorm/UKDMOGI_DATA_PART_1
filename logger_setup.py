"""
Logger setup for UKDMOGI Data Scraper

Provides centralized logging with:
- Console handler (INFO level) for user-friendly output
- File handler (DEBUG level) for detailed troubleshooting
- Timestamped log files for each run
- Third-party library noise suppression

Based on CHEF_NOVARTIS architecture
"""

import logging
import sys
from pathlib import Path
import config


def setup_logger(name="ukdmogi"):
    """
    Set up and configure logger with dual handlers

    Args:
        name: Logger name (default: "ukdmogi")

    Returns:
        logging.Logger: Configured logger instance
    """

    # Create logger
    logger = logging.getLogger(name)

    # Set root logger level based on DEBUG_MODE
    if config.DEBUG_MODE:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    # Prevent duplicate handlers if logger already configured
    if logger.handlers:
        return logger

    # Create formatters
    console_formatter = logging.Formatter(
        '%(levelname)s - %(message)s'
    )

    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # =============================================================================
    # CONSOLE HANDLER
    # =============================================================================
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, config.CONSOLE_LOG_LEVEL))
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # =============================================================================
    # FILE HANDLER
    # =============================================================================
    # Ensure logs directory exists
    log_dir = Path(config.LOGS_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(
        config.LOG_FILEPATH,
        mode='w',
        encoding='utf-8'
    )
    file_handler.setLevel(getattr(logging, config.FILE_LOG_LEVEL))
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # =============================================================================
    # SUPPRESS THIRD-PARTY LIBRARY NOISE
    # =============================================================================
    # Reduce log spam from common libraries

    # Selenium
    logging.getLogger('selenium').setLevel(logging.WARNING)
    logging.getLogger('selenium.webdriver.remote.remote_connection').setLevel(logging.WARNING)

    # urllib3 (used by requests and selenium)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)

    # xlrd (Excel reading)
    logging.getLogger('xlrd').setLevel(logging.WARNING)

    # openpyxl (Excel operations)
    logging.getLogger('openpyxl').setLevel(logging.WARNING)

    # PIL/Pillow (if used)
    logging.getLogger('PIL').setLevel(logging.WARNING)

    return logger


def log_section_header(logger, title):
    """
    Log a formatted section header for better readability

    Args:
        logger: Logger instance
        title: Section title
    """
    separator = "=" * 70
    logger.info("")
    logger.info(separator)
    logger.info(f"  {title}")
    logger.info(separator)


def log_subsection(logger, title):
    """
    Log a formatted subsection header

    Args:
        logger: Logger instance
        title: Subsection title
    """
    logger.info("")
    logger.info(f"--- {title} ---")


def log_step(logger, step_number, total_steps, description):
    """
    Log a workflow step in a standardized format

    Args:
        logger: Logger instance
        step_number: Current step number
        total_steps: Total number of steps
        description: Step description
    """
    logger.info(f"\n[Step {step_number}/{total_steps}] {description}")


def log_success(logger, message):
    """
    Log a success message with visual indicator

    Args:
        logger: Logger instance
        message: Success message
    """
    logger.info(f"[OK] {message}")


def log_error(logger, message, exception=None):
    """
    Log an error message with optional exception details

    Args:
        logger: Logger instance
        message: Error message
        exception: Optional exception object
    """
    if exception:
        logger.error(f"[ERROR] {message}: {str(exception)}")
        logger.debug("Exception details:", exc_info=True)
    else:
        logger.error(f"[ERROR] {message}")


def log_warning(logger, message):
    """
    Log a warning message with visual indicator

    Args:
        logger: Logger instance
        message: Warning message
    """
    logger.warning(f"[WARNING] {message}")


def log_data_summary(logger, data_dict):
    """
    Log a formatted data summary

    Args:
        logger: Logger instance
        data_dict: Dictionary of key-value pairs to log
    """
    logger.info("Data Summary:")
    for key, value in data_dict.items():
        logger.info(f"  {key}: {value}")


if __name__ == "__main__":
    # Test logger setup
    print("Testing logger setup...\n")

    # Initialize logger
    test_logger = setup_logger("test")

    # Test different log levels
    log_section_header(test_logger, "LOGGER TEST")

    test_logger.debug("This is a DEBUG message (only in file)")
    test_logger.info("This is an INFO message")
    test_logger.warning("This is a WARNING message")
    test_logger.error("This is an ERROR message")

    log_subsection(test_logger, "Testing Helper Functions")

    log_step(test_logger, 1, 3, "First step")
    log_success(test_logger, "Operation completed successfully")
    log_warning(test_logger, "This is a warning")
    log_error(test_logger, "This is an error")

    log_data_summary(test_logger, {
        "Files processed": 5,
        "Rows extracted": 100,
        "Errors": 0
    })

    print(f"\nLog file created at: {config.LOG_FILEPATH}")
    print("Check the log file for DEBUG messages not shown in console.")
