"""
Web Scraper for UKDMOGI Data Pipeline
UK Debt Management Office - Data Acquisition Module

Handles:
- Browser automation with Selenium
- Cookie consent handling
- Financial year selection
- Excel file download

Based on CHEF_NOVARTIS architecture
"""

import time
import os
import glob
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    ElementClickInterceptedException
)

import config
from logger_setup import setup_logger

# Initialize logger
logger = setup_logger(__name__)


class UKDMOScraper:
    """
    Scraper for UK Debt Management Office website
    Automates data extraction from Part 1: Outright Gilt Issuance Calendar
    """

    def __init__(self):
        """Initialize scraper with configuration"""
        self.driver = None
        self.download_dir = config.DOWNLOADS_DIR
        self.target_url = config.PART1_URL
        self.available_years = []

        # Ensure download directory exists
        os.makedirs(self.download_dir, exist_ok=True)

        logger.info("UKDMOScraper initialized")
        logger.debug(f"Download directory: {self.download_dir}")
        logger.debug(f"Target URL: {self.target_url}")

    def setup_driver(self):
        """
        Configure and initialize Chrome WebDriver with download preferences

        Returns:
            webdriver.Chrome: Configured Chrome driver instance
        """
        logger.info("Setting up Chrome WebDriver...")

        try:
            # Configure Chrome options
            chrome_options = webdriver.ChromeOptions()

            # Download preferences
            prefs = {
                "download.default_directory": self.download_dir,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
                "plugins.always_open_pdf_externally": True
            }
            chrome_options.add_experimental_option("prefs", prefs)

            # Headless mode (if configured)
            if config.HEADLESS_MODE:
                chrome_options.add_argument("--headless")
                chrome_options.add_argument("--disable-gpu")
                logger.info("Running in headless mode")

            # Additional Chrome arguments for stability
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--window-size=1920,1080")

            # Initialize driver
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(config.PAGE_LOAD_TIMEOUT)

            logger.info("Chrome WebDriver initialized successfully")
            return self.driver

        except Exception as e:
            logger.error(f"Failed to setup Chrome WebDriver: {str(e)}")
            raise

    def navigate_to_page(self):
        """
        Navigate to the DMO data page

        Returns:
            bool: True if successful, False otherwise
        """
        logger.info(f"Navigating to: {self.target_url}")

        try:
            self.driver.get(self.target_url)
            logger.info("Page loaded successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to navigate to page: {str(e)}")
            return False

    def handle_cookie_consent(self):
        """
        Handle cookie consent banner by clicking 'Accept Recommended Settings'

        Returns:
            bool: True if handled successfully, False otherwise
        """
        logger.info("Handling cookie consent banner...")

        try:
            # Wait for cookie consent button to appear
            cookie_button = WebDriverWait(self.driver, config.COOKIE_WAIT_TIMEOUT).until(
                EC.element_to_be_clickable((By.ID, config.COOKIE_CONSENT_BUTTON_ID))
            )

            logger.debug(f"Cookie consent button found: {config.COOKIE_CONSENT_BUTTON_ID}")

            # Click the button
            cookie_button.click()
            logger.info("[OK] Cookie consent accepted")

            # Wait a moment for banner to disappear
            time.sleep(1)

            return True

        except TimeoutException:
            logger.warning("Cookie consent button not found (may have been accepted previously)")
            return True  # Not critical - continue anyway

        except Exception as e:
            logger.warning(f"Could not handle cookie consent: {str(e)}")
            return True  # Not critical - continue anyway

    def select_financial_year(self, year=None):
        """
        Select financial year from dropdown

        Args:
            year: Financial year string (e.g., "2025-26")
                  If None, selects the latest (first option in dropdown)

        Returns:
            str: The selected financial year value
        """
        logger.info("Selecting financial year...")

        try:
            # Wait for dropdown to be present
            select_element = WebDriverWait(self.driver, config.ELEMENT_WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.NAME, config.FINANCIAL_YEAR_SELECT_NAME))
            )

            # Create Select object
            select = Select(select_element)

            # Get all available financial years (dropdown is latest-first)
            self.available_years = [opt.get_attribute("value") for opt in select.options]
            logger.info(f"Available financial years: {self.available_years[:5]}...")

            # Determine which year to select
            target_year = year if year else config.TARGET_FINANCIAL_YEAR

            if not target_year:
                # Select the latest (first option)
                select.select_by_index(0)
                selected_year = select.first_selected_option.get_attribute("value")
                logger.info(f"[OK] Selected latest financial year: {selected_year}")
                return selected_year

            # Verify the target year exists in the dropdown
            if target_year not in self.available_years:
                logger.warning(f"Financial year '{target_year}' not found in dropdown. Available: {self.available_years[:5]}...")
                select.select_by_index(0)
                selected_year = select.first_selected_option.get_attribute("value")
                logger.info(f"[OK] Fell back to latest year: {selected_year}")
                return selected_year

            select.select_by_value(target_year)
            logger.info(f"[OK] Selected financial year: {target_year}")
            return target_year

        except Exception as e:
            logger.error(f"Failed to select financial year: {str(e)}")
            raise

    def click_excel_download(self):
        """
        Click the Excel download button to trigger file download

        Returns:
            bool: True if clicked successfully, False otherwise
        """
        logger.info("Clicking Excel download button...")

        try:
            # Wait for Excel button to be clickable
            excel_button = WebDriverWait(self.driver, config.ELEMENT_WAIT_TIMEOUT).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Excel')]"))
            )

            logger.debug("Excel button found")

            # Scroll button into view (in case it's not visible)
            self.driver.execute_script("arguments[0].scrollIntoView(true);", excel_button)
            time.sleep(0.5)

            # Click the button
            excel_button.click()
            logger.info("[OK] Excel download button clicked")

            return True

        except TimeoutException:
            logger.error("Excel download button not found within timeout period")
            return False

        except ElementClickInterceptedException:
            # Try JavaScript click if normal click is intercepted
            logger.warning("Normal click intercepted, trying JavaScript click...")
            try:
                excel_button = self.driver.find_element(By.XPATH, "//button[contains(text(), 'Excel')]")
                self.driver.execute_script("arguments[0].click();", excel_button)
                logger.info("[OK] Excel download triggered via JavaScript")
                return True
            except Exception as js_error:
                logger.error(f"JavaScript click also failed: {str(js_error)}")
                return False

        except Exception as e:
            logger.error(f"Failed to click Excel button: {str(e)}")
            return False

    def wait_for_download(self, timeout=None):
        """
        Wait for file download to complete

        Args:
            timeout: Maximum time to wait in seconds (default: from config)

        Returns:
            str: Path to downloaded file, or None if failed
        """
        if timeout is None:
            timeout = config.DOWNLOAD_WAIT_TIMEOUT

        logger.info(f"Waiting for download to complete (timeout: {timeout}s)...")

        # Track initial files
        initial_files = set(glob.glob(os.path.join(self.download_dir, "*")))
        logger.debug(f"Initial files in directory: {len(initial_files)}")

        start_time = time.time()
        downloaded_file = None

        while time.time() - start_time < timeout:
            # Check for new files (not .crdownload or .tmp)
            current_files = set(glob.glob(os.path.join(self.download_dir, "*")))
            new_files = current_files - initial_files

            # Filter out incomplete downloads
            complete_files = [
                f for f in new_files
                if not f.endswith('.crdownload') and not f.endswith('.tmp')
            ]

            if complete_files:
                downloaded_file = complete_files[0]
                file_size = os.path.getsize(downloaded_file)

                # Validate file size
                if file_size >= config.MIN_FILE_SIZE:
                    logger.info(f"[OK] Download complete: {os.path.basename(downloaded_file)}")
                    logger.debug(f"File size: {file_size:,} bytes")
                    return downloaded_file
                else:
                    logger.warning(f"Downloaded file too small: {file_size} bytes")

            time.sleep(1)

        logger.error(f"Download timeout after {timeout} seconds")
        return None

    def close_driver(self):
        """Close the WebDriver and cleanup"""
        if self.driver:
            logger.info("Closing browser...")
            try:
                self.driver.quit()
                logger.info("Browser closed successfully")
            except Exception as e:
                logger.warning(f"Error closing browser: {str(e)}")

    def open_browser(self):
        """
        Open browser, navigate to page, and handle cookie consent.
        Call this once before downloading for one or more financial years.

        Returns:
            list: Available financial years from the dropdown (latest first)
        """
        self.setup_driver()

        if not self.navigate_to_page():
            raise Exception("Failed to navigate to page")

        self.handle_cookie_consent()

        # Read available years from the dropdown (without selecting yet)
        select_element = WebDriverWait(self.driver, config.ELEMENT_WAIT_TIMEOUT).until(
            EC.presence_of_element_located((By.NAME, config.FINANCIAL_YEAR_SELECT_NAME))
        )
        select = Select(select_element)
        self.available_years = [opt.get_attribute("value") for opt in select.options]
        logger.info(f"Available financial years: {self.available_years[:5]}...")

        return self.available_years

    def _set_download_dir(self, directory):
        """
        Update Chrome's download directory at runtime via DevTools Protocol.

        Args:
            directory: Absolute path to the new download directory
        """
        os.makedirs(directory, exist_ok=True)
        self.download_dir = directory

        # Use Chrome DevTools Protocol to change download path on the fly
        self.driver.execute_cdp_cmd("Page.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": directory
        })
        logger.debug(f"Download directory set to: {directory}")

    def download_for_year(self, financial_year=None):
        """
        Download Excel file for a specific financial year.
        Browser must already be open (call open_browser first).
        Re-navigates to ensure clean page state for each download.
        Each FY downloads into its own subfolder (e.g. downloads/<timestamp>/2025-26/).

        Args:
            financial_year: Financial year string (e.g., "2025-26")
                            If None, selects the latest (first option)

        Returns:
            dict: {"success": bool, "file_path": str, "financial_year": str, "error": str}
        """
        result = {
            "success": False,
            "file_path": None,
            "financial_year": None,
            "error": None
        }

        try:
            # Re-navigate to get a clean page (cookies already accepted in session)
            if not self.navigate_to_page():
                result["error"] = "Failed to navigate to page"
                return result

            # Select financial year
            selected_year = self.select_financial_year(financial_year)
            result["financial_year"] = selected_year

            # Set download dir to FY-specific subfolder
            fy_download_dir = os.path.join(config.DOWNLOADS_DIR, selected_year)
            self._set_download_dir(fy_download_dir)

            # Click Excel download
            if not self.click_excel_download():
                result["error"] = "Failed to click Excel download button"
                return result

            # Wait for download
            downloaded_file = self.wait_for_download()
            if not downloaded_file:
                result["error"] = "Download timeout or failed"
                return result

            result["success"] = True
            result["file_path"] = downloaded_file
            logger.info(f"[OK] Downloaded data for {selected_year}")
            return result

        except Exception as e:
            logger.error(f"Download failed for {financial_year}: {str(e)}", exc_info=True)
            result["error"] = str(e)
            return result

    def scrape_part1(self, financial_year=None):
        """
        Convenience method: single financial year scrape (opens and closes browser).

        Args:
            financial_year: Optional specific financial year to scrape

        Returns:
            dict: Result dictionary with status and file path
        """
        try:
            self.open_browser()
            result = self.download_for_year(financial_year)
            result["available_years"] = list(self.available_years)
            return result

        except Exception as e:
            logger.error(f"Scraping failed: {str(e)}", exc_info=True)
            return {
                "success": False,
                "file_path": None,
                "financial_year": None,
                "available_years": list(self.available_years),
                "error": str(e)
            }

        finally:
            self.close_driver()


def main():
    """
    Test function for scraper module
    """
    from logger_setup import log_section_header

    logger = setup_logger("scraper_test")
    log_section_header(logger, "UKDMO SCRAPER TEST")

    # Create scraper instance
    scraper = UKDMOScraper()

    # Run scraping
    logger.info("Starting Part 1 scraping test...")
    result = scraper.scrape_part1()

    # Print results
    logger.info("\n" + "=" * 70)
    logger.info("SCRAPING RESULTS")
    logger.info("=" * 70)
    logger.info(f"Success: {result['success']}")
    logger.info(f"Financial Year: {result['financial_year']}")
    logger.info(f"Downloaded File: {result['file_path']}")
    if result['error']:
        logger.error(f"Error: {result['error']}")

    return result


if __name__ == "__main__":
    main()
