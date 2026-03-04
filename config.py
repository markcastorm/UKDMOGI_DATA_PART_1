"""
Configuration file for UKDMOGI Data Scraper
UK Debt Management Office - Gilt Issuance Data Pipeline

This configuration follows the proven architecture from CHEF_NOVARTIS project.
All settings, column mappings, and metadata are centralized here.
"""

import os
from datetime import datetime

# =============================================================================
# PROJECT METADATA
# =============================================================================
PROJECT_NAME = "UKDMOGI"
PROVIDER = "AfricaAI"
SOURCE = "UKDMO"
SOURCE_DESCRIPTION = "United Kingdom Debt Management Office"
COUNTRY = "GBR"
CURRENCY = "GBP"

# =============================================================================
# TIMESTAMP CONFIGURATION
# =============================================================================
# Generate timestamp once at import time for consistent file naming across modules
RUN_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
DATE_STAMP = datetime.now().strftime("%Y%m%d")

# =============================================================================
# DIRECTORY STRUCTURE
# =============================================================================
# Base directory (project root)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Timestamped folders for this run
DOWNLOADS_DIR = os.path.join(BASE_DIR, "downloads", RUN_TIMESTAMP)
OUTPUT_DIR = os.path.join(BASE_DIR, "output", RUN_TIMESTAMP)
LOGS_DIR = os.path.join(BASE_DIR, "logs", RUN_TIMESTAMP)

# Latest folder for quick access to current results
LATEST_OUTPUT_DIR = os.path.join(BASE_DIR, "output", "latest")

# =============================================================================
# PART 1: OUTRIGHT GILT ISSUANCE CALENDAR
# =============================================================================

# Data Source URLs
PART1_URL = "https://www.dmo.gov.uk/data/pdfdatareport?reportCode=D5D"
PART1_REPORT_CODE = "D5D"

# Financial Year Selection
# Options:
#   - None: Auto-detect current financial year (e.g. "2025-26" if today is in Apr 2025 - Mar 2026)
#   - "2025-26": Select specific financial year
#   - "2024-25", "2023-24", etc.
TARGET_FINANCIAL_YEAR = None  # Set to None for auto-detect, or e.g. "2024-25" for specific year

# Column names in downloaded Excel file
EXCEL_DATE_COLUMN = "Operation Date"
EXCEL_CASH_RAISED_COLUMN = "Cash Raised        (£ million)"

# =============================================================================
# OUTPUT COLUMN CONFIGURATION (PART 1)
# =============================================================================
# These MUST match the exact format from the manual data file
# The order and naming are CRITICAL - do not modify

# Row 1 headers (code mnemonics)
OUTPUT_CODE_MNEMONIC = "UKDMOGI.CASHRAISED.B"

# Row 2 headers (descriptions)
OUTPUT_DESCRIPTION = "Cash Raised"

# =============================================================================
# METADATA CONFIGURATION (PART 1)
# =============================================================================
# All metadata fields for PART 1 dataset
# These values are written to the META CSV file

METADATA_COLUMNS = [
    "CODE_MNEMONIC",
    "DESCRIPTION",
    "FREQUENCY",
    "MULTIPLIER",
    "AGGREGATION_TYPE",
    "UNIT_TYPE",
    "DATA_TYPE",
    "DATA_UNIT",
    "SEASONALLY_ADJUSTED",
    "ANNUALIZED",
    "PROVIDER_MEASURE_URL",
    "PROVIDER",
    "SOURCE",
    "SOURCE_DESCRIPTION",
    "COUNTRY",
    "DATASET"
]

METADATA_PART1 = {
    "CODE_MNEMONIC": "UKDMOGI.CASHRAISED",
    "DESCRIPTION": "Cash Raised",
    "FREQUENCY": "B",  # Business day
    "MULTIPLIER": "6",
    "AGGREGATION_TYPE": "UNDEFINED",
    "UNIT_TYPE": "FLOW",
    "DATA_TYPE": "CURRENCY",
    "DATA_UNIT": CURRENCY,
    "SEASONALLY_ADJUSTED": "NSA",
    "ANNUALIZED": "FALSE",
    "PROVIDER_MEASURE_URL": PART1_URL,
    "PROVIDER": PROVIDER,
    "SOURCE": SOURCE,
    "SOURCE_DESCRIPTION": SOURCE_DESCRIPTION,
    "COUNTRY": COUNTRY,
    "DATASET": PROJECT_NAME
}

# =============================================================================
# WEB SCRAPING CONFIGURATION
# =============================================================================

# Cookie Consent Selectors
COOKIE_CONSENT_BUTTON_ID = "ccc-recommended-settings"
COOKIE_CLOSE_BUTTON_ID = "ccc-close"

# Form Selectors (use name attribute - stable across sessions, unlike the dynamic GUID-based IDs)
FINANCIAL_YEAR_SELECT_NAME = "Financial Year"

# Export Button Selector (JavaScript-triggered button)
EXCEL_BUTTON_SELECTOR = "button.btn.btn-secondary[onclick*='GenerateDownloadDataReport']"

# Wait times (seconds)
COOKIE_WAIT_TIMEOUT = 10
PAGE_LOAD_TIMEOUT = 30
DOWNLOAD_WAIT_TIMEOUT = 60
ELEMENT_WAIT_TIMEOUT = 20

# =============================================================================
# FILE NAMING CONVENTIONS
# =============================================================================

# Output file name patterns (Excel format)
DATA_FILENAME_PART1 = f"UKDMOGI_DATA_PART_1_{DATE_STAMP}.xlsx"
META_FILENAME_PART1 = f"UKDMOGI_META_PART_1_{DATE_STAMP}.xlsx"

# Downloaded file pattern (for identification)
DOWNLOADED_FILE_PATTERN = "*Outright Gilt Issuance Calendar*.xls"

# =============================================================================
# DATA PROCESSING CONFIGURATION
# =============================================================================

# Date format in output (YYYY-MM-DD)
OUTPUT_DATE_FORMAT = "%Y-%m-%d"

# Date formats to try when parsing Excel dates
INPUT_DATE_FORMATS = [
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%d-%m-%Y",
    "%Y/%m/%d"
]

# Numeric formatting
DECIMAL_PLACES = 4  # Number of decimal places for cash values

# Handle blank values
BLANK_VALUE_REPLACEMENT = ""  # Keep as blank in CSV

# =============================================================================
# BROWSER CONFIGURATION
# =============================================================================

# Selenium/Browser settings
HEADLESS_MODE = False  # Set to True for unattended operation
BROWSER_DOWNLOAD_DIR = DOWNLOADS_DIR  # Where browser downloads files

# Chrome options
CHROME_OPTIONS = {
    "download.default_directory": DOWNLOADS_DIR,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True,
    "plugins.always_open_pdf_externally": True  # Don't open in browser
}

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

# Log file name
LOG_FILENAME = f"ukdmogi_{RUN_TIMESTAMP}.log"
LOG_FILEPATH = os.path.join(LOGS_DIR, LOG_FILENAME)

# Logging levels
CONSOLE_LOG_LEVEL = "INFO"  # INFO for user-friendly output
FILE_LOG_LEVEL = "DEBUG"    # DEBUG for detailed troubleshooting

# Enable/disable debug mode
DEBUG_MODE = True

# =============================================================================
# VALIDATION CONFIGURATION
# =============================================================================

# Require minimum number of data rows
MIN_DATA_ROWS = 1

# Validate downloaded file size (bytes)
MIN_FILE_SIZE = 1024  # 1KB minimum

# =============================================================================
# ERROR HANDLING
# =============================================================================

# Continue processing on non-critical errors
CONTINUE_ON_ERROR = False

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# =============================================================================
# PART 2 CONFIGURATION (Future Implementation)
# =============================================================================
# These will be used when implementing Part 2 scraper

PART2_URL = "https://www.dmo.gov.uk/data/ExportReport?reportCode=D1C"
PART2_REPORT_CODE = "D1C"
PART2_COLUMN_NAME = "Nominal amount outstanding at redemption (£ million)"
PART2_MULTIPLY_BY_NEGATIVE = True  # Values should be negative

# Part 2 weekend consolidation: If data published on weekend, save as Monday
PART2_WEEKEND_CONSOLIDATION = True

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def create_directories():
    """Create all necessary directories if they don't exist"""
    directories = [
        DOWNLOADS_DIR,
        OUTPUT_DIR,
        LOGS_DIR,
        LATEST_OUTPUT_DIR
    ]

    for directory in directories:
        os.makedirs(directory, exist_ok=True)

    return directories


def get_config_summary():
    """Return a formatted summary of key configuration settings"""
    summary = f"""
    ╔══════════════════════════════════════════════════════════════════╗
    ║           UKDMOGI DATA SCRAPER - CONFIGURATION SUMMARY          ║
    ╚══════════════════════════════════════════════════════════════════╝

    Project:          {PROJECT_NAME}
    Provider:         {PROVIDER}
    Source:           {SOURCE_DESCRIPTION}

    Target URL:       {PART1_URL}
    Financial Year:   {TARGET_FINANCIAL_YEAR if TARGET_FINANCIAL_YEAR else 'Latest (default)'}

    Output Directory: {OUTPUT_DIR}
    Downloads:        {DOWNLOADS_DIR}
    Logs:             {LOGS_DIR}

    Data File:        {DATA_FILENAME_PART1}
    Meta File:        {META_FILENAME_PART1}

    Headless Mode:    {HEADLESS_MODE}
    Debug Mode:       {DEBUG_MODE}
    """
    return summary


if __name__ == "__main__":
    # Test configuration by creating directories and printing summary
    create_directories()
    print(get_config_summary())
