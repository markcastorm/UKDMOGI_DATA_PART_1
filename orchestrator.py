"""
Orchestrator for UKDMOGI Data Pipeline
Main workflow coordinator

Coordinates the complete data pipeline:
1. Web scraping (download Excel files)
2. Data parsing (extract and transform)
3. File generation (create DATA and META files)

Based on CHEF_NOVARTIS architecture
"""

import sys
import signal
from datetime import datetime

import config
from logger_setup import (
    setup_logger,
    log_section_header,
    log_step,
    log_success,
    log_error
)
from scraper import UKDMOScraper
from parser import UKDMOParser
from file_generator import UKDMOFileGenerator

# Initialize logger
logger = setup_logger("orchestrator")

# Global flag for graceful shutdown
shutdown_requested = False


def signal_handler(signum, frame):
    """Handle CTRL+C gracefully"""
    global shutdown_requested
    logger.warning("\n\nShutdown requested by user (CTRL+C)")
    shutdown_requested = True
    sys.exit(130)  # Standard exit code for SIGINT


# Register signal handler
signal.signal(signal.SIGINT, signal_handler)


def print_banner():
    """Print application banner"""
    banner = """
    ==================================================================

              UK DEBT MANAGEMENT OFFICE DATA SCRAPER
                    Gilt Issuance Data Pipeline

                         Part 1: Outright Gilt
                       Issuance Calendar

    ==================================================================
    """
    print(banner)


def print_configuration():
    """Print current configuration summary"""
    fy_display = config.TARGET_FINANCIAL_YEAR if config.TARGET_FINANCIAL_YEAR else 'Auto (latest with data)'
    config_text = f"""
    Configuration:
    -----------------------------------------------------------------
    Project:           {config.PROJECT_NAME}
    Provider:          {config.PROVIDER}
    Source:            {config.SOURCE_DESCRIPTION}

    Target URL:        {config.PART1_URL}
    Financial Year:    {fy_display}

    Run Timestamp:     {config.RUN_TIMESTAMP}
    Output Directory:  {config.OUTPUT_DIR}
    Downloads:         {config.DOWNLOADS_DIR}
    Logs:              {config.LOG_FILEPATH}

    Headless Mode:     {config.HEADLESS_MODE}
    Debug Mode:        {config.DEBUG_MODE}
    -----------------------------------------------------------------
    """
    print(config_text)


def setup_environment():
    """
    Setup environment and create necessary directories

    Returns:
        bool: True if successful, False otherwise
    """
    log_step(logger, 0, 3, "Setting up environment")

    try:
        # Create directories
        config.create_directories()
        log_success(logger, "Directories created")

        return True

    except Exception as e:
        log_error(logger, "Failed to setup environment", e)
        return False


def has_cash_data(parsed_data):
    """
    Check if parsed data contains any rows with actual cash values.

    Args:
        parsed_data: List of {"date": str, "cash_raised": float/None}

    Returns:
        bool: True if at least one row has a non-None cash_raised value
    """
    return any(row['cash_raised'] is not None for row in parsed_data)


def run_parser(excel_file_path):
    """
    Execute parsing to extract data from Excel file

    Args:
        excel_file_path: Path to downloaded Excel file

    Returns:
        dict: Parser result
    """
    try:
        parser = UKDMOParser()
        result = parser.parse_excel_file(excel_file_path)

        if result['success']:
            log_success(logger, f"Data parsed: {result['row_count']} rows extracted")

            # Log date range
            if result['data']:
                first_date = result['data'][0]['date']
                last_date = result['data'][-1]['date']
                logger.info(f"Date range: {first_date} to {last_date}")
        else:
            log_error(logger, f"Parsing failed: {result['error']}")

        return result

    except Exception as e:
        log_error(logger, "Parsing stage failed", e)
        return {"success": False, "error": str(e)}


def run_generator(parsed_data):
    """
    Execute file generation to create DATA and META files

    Args:
        parsed_data: List of parsed data rows

    Returns:
        dict: Generator result
    """
    try:
        generator = UKDMOFileGenerator()
        result = generator.generate_files(parsed_data)

        if result['success']:
            log_success(logger, "Output files generated successfully")
            logger.info(f"DATA: {result['data_file']}")
            logger.info(f"META: {result['meta_file']}")
        else:
            log_error(logger, f"File generation failed: {result['error']}")

        return result

    except Exception as e:
        log_error(logger, "File generation stage failed", e)
        return {"success": False, "error": str(e)}


def main(financial_year=None):
    """
    Main orchestration workflow

    When financial_year is None (auto mode):
      1. Open browser once
      2. Try the latest financial year from the dropdown
      3. If it has no cash data, try the next financial year (same browser session)
      4. Only tries up to 2 financial years

    When financial_year is set explicitly, only that year is tried.

    Args:
        financial_year: Optional specific financial year to process

    Returns:
        int: Exit code (0 = success, 1 = failure, 130 = interrupted)
    """
    start_time = datetime.now()

    # Print banner and configuration
    print_banner()
    print_configuration()

    log_section_header(logger, "UKDMOGI DATA PIPELINE - PART 1")
    logger.info(f"Pipeline started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # Setup environment
        if not setup_environment():
            logger.error("Environment setup failed. Exiting.")
            return 1

        if shutdown_requested:
            return 130

        auto_mode = financial_year is None and config.TARGET_FINANCIAL_YEAR is None

        # =====================================================================
        # STAGE 1: WEB SCRAPING
        # =====================================================================
        log_step(logger, 1, 3, "Web Scraping - Downloading Excel file")

        scraper = UKDMOScraper()

        try:
            available_years = scraper.open_browser()

            # Determine which years to try
            if auto_mode:
                # Auto mode: try latest first, cascade to next if no data
                years_to_try = available_years[:2]
                logger.info(f"Auto mode: will try up to {len(years_to_try)} financial years")
            else:
                # Explicit mode: only the specified year
                years_to_try = [financial_year or config.TARGET_FINANCIAL_YEAR]

            # Try each financial year until we find one with cash data
            scraper_result = None
            parser_result = None
            selected_year = None

            for i, fy in enumerate(years_to_try):
                if shutdown_requested:
                    return 130

                if i > 0:
                    logger.info(f"No cash data in {selected_year}, trying next: {fy}")

                # Download for this FY
                scraper_result = scraper.download_for_year(fy)

                if not scraper_result['success']:
                    log_error(logger, f"Scraping failed for {fy}: {scraper_result['error']}")
                    continue

                selected_year = scraper_result['financial_year']
                log_success(logger, f"Excel file downloaded: {selected_year}")
                logger.info(f"File: {scraper_result['file_path']}")

                # Parse immediately to check for cash data
                log_step(logger, 2, 3, f"Data Parsing - Checking {selected_year}")
                parser_result = run_parser(scraper_result['file_path'])

                if not parser_result['success']:
                    log_error(logger, f"Parsing failed for {selected_year}")
                    continue

                # Check for cash data
                if has_cash_data(parser_result['data']):
                    logger.info(f"[OK] Cash data found for {selected_year}")
                    break
                else:
                    logger.warning(f"No cash data in {selected_year} (future dates only)")
                    parser_result = None  # Reset so we know to continue

            # Done trying — close browser
        finally:
            scraper.close_driver()

        # Check if we found data
        if not scraper_result or not scraper_result['success']:
            logger.error("Pipeline failed at scraping stage")
            return 1

        if not parser_result or not parser_result['success']:
            if auto_mode:
                logger.error(f"No cash data found in any of: {', '.join(years_to_try)}")
            else:
                logger.error("Pipeline failed at parsing stage")
            return 1

        if shutdown_requested:
            return 130

        # =====================================================================
        # STAGE 3: FILE GENERATION
        # =====================================================================
        log_step(logger, 3, 3, "File Generation - Creating DATA and META Excel files")
        generator_result = run_generator(parser_result['data'])

        if not generator_result['success']:
            logger.error("Pipeline failed at file generation stage")
            return 1

        # =====================================================================
        # PIPELINE COMPLETE
        # =====================================================================
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        log_section_header(logger, "PIPELINE COMPLETED SUCCESSFULLY")

        logger.info("\n[OK] All stages completed successfully!\n")
        logger.info("Summary:")
        logger.info(f"  Financial Year:  {scraper_result['financial_year']}")
        logger.info(f"  Data Rows:       {parser_result['row_count']}")
        logger.info(f"  DATA File:       {generator_result['data_file']}")
        logger.info(f"  META File:       {generator_result['meta_file']}")
        logger.info(f"  Duration:        {duration:.2f} seconds")
        logger.info(f"  Log File:        {config.LOG_FILEPATH}")

        logger.info("\nOutput files saved to:")
        logger.info(f"  Timestamped:     {config.OUTPUT_DIR}")
        logger.info(f"  Latest:          {config.LATEST_OUTPUT_DIR}")

        return 0

    except KeyboardInterrupt:
        logger.warning("\nPipeline interrupted by user")
        return 130

    except Exception as e:
        log_error(logger, "Pipeline failed with unexpected error", e)
        logger.error("\nPipeline failed. Check logs for details.")
        logger.error(f"Log file: {config.LOG_FILEPATH}")
        return 1


if __name__ == "__main__":
    """
    Entry point for UKDMOGI data pipeline

    Usage:
        python orchestrator.py              # Use latest financial year
        python orchestrator.py 2024-25      # Use specific financial year
    """

    # Check for financial year argument
    target_year = None
    if len(sys.argv) > 1:
        target_year = sys.argv[1]
        logger.info(f"Target financial year specified: {target_year}")

    # Run pipeline
    exit_code = main(target_year)

    # Exit with appropriate code
    sys.exit(exit_code)
