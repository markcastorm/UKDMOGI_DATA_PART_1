"""
Data Parser for UKDMOGI Pipeline
Extracts and transforms data from downloaded Excel files

Handles:
- Excel file reading (xlrd for .xls files)
- Date parsing and formatting
- Cash Raised value extraction
- Data validation and cleaning

Based on CHEF_NOVARTIS architecture
"""

import os
from datetime import datetime
import pandas as pd
import xlrd
from xlrd import XLRDError

import config
from logger_setup import setup_logger

# Initialize logger
logger = setup_logger(__name__)


class UKDMOParser:
    """
    Parser for UK Debt Management Office Excel files
    Extracts Operation Date and Cash Raised data
    """

    def __init__(self):
        """Initialize parser"""
        self.date_column = config.EXCEL_DATE_COLUMN
        self.cash_column = config.EXCEL_CASH_RAISED_COLUMN

        logger.info("UKDMOParser initialized")
        logger.debug(f"Date column: {self.date_column}")
        logger.debug(f"Cash column: {self.cash_column}")

    def parse_date(self, date_value, workbook=None):
        """
        Parse date from Excel file (handles multiple formats)

        Args:
            date_value: Raw date value from Excel
            workbook: xlrd workbook object (for date mode)

        Returns:
            str: Formatted date string (YYYY-MM-DD) or None
        """
        if pd.isna(date_value) or date_value == '' or date_value is None:
            return None

        try:
            # Case 1: Already a datetime object (from pandas)
            if isinstance(date_value, datetime):
                return date_value.strftime(config.OUTPUT_DATE_FORMAT)

            # Case 2: String date - try multiple formats
            if isinstance(date_value, str):
                for date_format in config.INPUT_DATE_FORMATS:
                    try:
                        parsed_date = datetime.strptime(date_value, date_format)
                        return parsed_date.strftime(config.OUTPUT_DATE_FORMAT)
                    except ValueError:
                        continue

            # Case 3: Excel serial date number
            if isinstance(date_value, (int, float)):
                if workbook:
                    # Use xlrd's date conversion
                    date_tuple = xlrd.xldate_as_tuple(date_value, workbook.datemode)
                    parsed_date = datetime(*date_tuple)
                    return parsed_date.strftime(config.OUTPUT_DATE_FORMAT)
                else:
                    # Fallback: Excel epoch (1899-12-30)
                    excel_epoch = datetime(1899, 12, 30)
                    parsed_date = excel_epoch + pd.Timedelta(days=date_value)
                    return parsed_date.strftime(config.OUTPUT_DATE_FORMAT)

        except Exception as e:
            logger.debug(f"Could not parse date '{date_value}': {str(e)}")
            return None

        logger.warning(f"Unrecognized date format: {date_value} (type: {type(date_value)})")
        return None

    def parse_cash_value(self, cash_value):
        """
        Parse cash raised value from Excel

        Args:
            cash_value: Raw cash value from Excel

        Returns:
            float: Parsed value, or None for blank cells
        """
        if pd.isna(cash_value) or cash_value == '' or cash_value is None:
            return None

        try:
            # Remove commas and convert to float
            if isinstance(cash_value, str):
                # Remove commas, spaces, and currency symbols
                cleaned = cash_value.replace(',', '').replace(' ', '').replace('£', '').strip()
                if cleaned == '' or cleaned == '-':
                    return None
                return float(cleaned)

            # Already a number
            if isinstance(cash_value, (int, float)):
                return float(cash_value)

        except Exception as e:
            logger.warning(f"Could not parse cash value '{cash_value}': {str(e)}")
            return None

        return None

    def parse_excel_file(self, file_path):
        """
        Parse Excel file and extract Operation Date and Cash Raised

        Args:
            file_path: Path to Excel file

        Returns:
            dict: Parsed data
                  {
                      "success": bool,
                      "data": list of {"date": str, "cash_raised": float},
                      "row_count": int,
                      "error": str
                  }
        """
        result = {
            "success": False,
            "data": [],
            "row_count": 0,
            "error": None
        }

        if not os.path.exists(file_path):
            result["error"] = f"File not found: {file_path}"
            logger.error(result["error"])
            return result

        logger.info(f"Parsing Excel file: {os.path.basename(file_path)}")

        try:
            # SMART HEADER DETECTION: Find the header row automatically
            # Read first 20 rows without assuming header position
            df_preview = pd.read_excel(file_path, engine='xlrd', header=None, nrows=20)

            # Search for header row by looking for expected column names
            header_row = None
            for idx, row in df_preview.iterrows():
                row_str = ' '.join([str(cell).lower() for cell in row if pd.notna(cell)])

                # Check if this row contains our expected column names
                if 'operation date' in row_str and 'cash raised' in row_str:
                    header_row = idx
                    logger.info(f"Header row automatically detected at row {idx}")
                    break

            if header_row is None:
                result["error"] = "Could not find header row with expected columns (Operation Date, Cash Raised)"
                logger.error(result["error"])
                return result

            # Now read the full file with the correct header row
            df = pd.read_excel(file_path, engine='xlrd', header=header_row)

            logger.debug(f"Excel file loaded. Shape: {df.shape}")
            logger.debug(f"Columns: {df.columns.tolist()}")

            # Find the date and cash columns dynamically
            date_col_name = None
            cash_col_name = None

            for col in df.columns:
                col_str = str(col).strip()
                if self.date_column.lower() in col_str.lower():
                    date_col_name = col
                    logger.debug(f"Date column found: '{col}'")

                # Match cash column (may have extra spaces)
                if "cash raised" in col_str.lower() and "million" in col_str.lower():
                    cash_col_name = col
                    logger.debug(f"Cash column found: '{col}'")

            if date_col_name is None:
                result["error"] = f"Date column '{self.date_column}' not found in Excel file"
                logger.error(result["error"])
                logger.debug(f"Available columns: {df.columns.tolist()}")
                return result

            if cash_col_name is None:
                result["error"] = f"Cash column '{self.cash_column}' not found in Excel file"
                logger.error(result["error"])
                logger.debug(f"Available columns: {df.columns.tolist()}")
                return result

            # Open workbook for date parsing (if needed)
            workbook = None
            try:
                workbook = xlrd.open_workbook(file_path)
            except Exception as wb_error:
                logger.debug(f"Could not open workbook with xlrd: {wb_error}")

            # Extract data row by row
            parsed_data = []
            skipped_rows = 0

            for idx, row in df.iterrows():
                date_value = row[date_col_name]
                cash_value = row[cash_col_name]

                # Parse date
                parsed_date = self.parse_date(date_value, workbook)

                # Parse cash value
                parsed_cash = self.parse_cash_value(cash_value)

                # Only include rows with valid dates
                if parsed_date:
                    parsed_data.append({
                        "date": parsed_date,
                        "cash_raised": parsed_cash  # Can be None for blank cells
                    })
                else:
                    # Skip rows without valid dates (likely headers or empty rows)
                    if idx < 10:  # Only log first few skips to avoid spam
                        logger.debug(f"Skipped row {idx}: date={date_value}, cash={cash_value}")
                    skipped_rows += 1

            # Sort by date (ascending)
            parsed_data.sort(key=lambda x: x["date"])

            # Log summary
            logger.info(f"[OK] Parsed {len(parsed_data)} data rows")
            logger.debug(f"Skipped {skipped_rows} rows (no valid date)")

            if parsed_data:
                logger.debug(f"Date range: {parsed_data[0]['date']} to {parsed_data[-1]['date']}")

            # Validate minimum rows
            if len(parsed_data) < config.MIN_DATA_ROWS:
                result["error"] = f"Insufficient data rows: {len(parsed_data)} (minimum: {config.MIN_DATA_ROWS})"
                logger.error(result["error"])
                return result

            # Success
            result["success"] = True
            result["data"] = parsed_data
            result["row_count"] = len(parsed_data)

            return result

        except XLRDError as e:
            result["error"] = f"Excel parsing error: {str(e)}"
            logger.error(result["error"])
            return result

        except Exception as e:
            result["error"] = f"Unexpected error parsing Excel: {str(e)}"
            logger.error(result["error"], exc_info=True)
            return result


def main():
    """
    Test function for parser module
    """
    from logger_setup import log_section_header
    import glob

    logger = setup_logger("parser_test")
    log_section_header(logger, "UKDMO PARSER TEST")

    # Find a test Excel file
    test_files = glob.glob(os.path.join(config.BASE_DIR, "project information", "*.xls"))

    if not test_files:
        logger.error("No test Excel files found in 'project information' folder")
        return

    test_file = test_files[0]
    logger.info(f"Testing with file: {os.path.basename(test_file)}")

    # Create parser and test
    parser = UKDMOParser()
    result = parser.parse_excel_file(test_file)

    # Print results
    logger.info("\n" + "=" * 70)
    logger.info("PARSING RESULTS")
    logger.info("=" * 70)
    logger.info(f"Success: {result['success']}")
    logger.info(f"Rows parsed: {result['row_count']}")

    if result['error']:
        logger.error(f"Error: {result['error']}")

    if result['success'] and result['data']:
        logger.info("\nFirst 5 rows:")
        for i, row in enumerate(result['data'][:5]):
            logger.info(f"  {row['date']}: {row['cash_raised']}")

        logger.info("\nLast 5 rows:")
        for row in result['data'][-5:]:
            logger.info(f"  {row['date']}: {row['cash_raised']}")

    return result


if __name__ == "__main__":
    main()
