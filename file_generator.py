"""
File Generator for UKDMOGI Pipeline
Generates DATA and META Excel files in required format

Handles:
- DATA Excel generation with exact header format
- META Excel generation with all metadata fields
- File copying to 'latest' folder
- Timestamped file naming
- Filtering out trailing empty rows (future dates with no data)

Based on CHEF_NOVARTIS architecture
"""

import os
import shutil
from pathlib import Path
import pandas as pd

import config
from logger_setup import setup_logger

# Initialize logger
logger = setup_logger(__name__)


class UKDMOFileGenerator:
    """
    File generator for UKDMOGI data pipeline
    Creates DATA and META CSV files in standardized format
    """

    def __init__(self):
        """Initialize file generator"""
        self.output_dir = config.OUTPUT_DIR
        self.latest_dir = config.LATEST_OUTPUT_DIR

        # Ensure directories exist
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.latest_dir, exist_ok=True)

        logger.info("UKDMOFileGenerator initialized")
        logger.debug(f"Output directory: {self.output_dir}")
        logger.debug(f"Latest directory: {self.latest_dir}")

    def filter_trailing_empty_rows(self, parsed_data):
        """
        Filter out trailing rows with no cash data (future dates)

        Args:
            parsed_data: List of {"date": str, "cash_raised": float/None}

        Returns:
            list: Filtered data up to last row with actual cash value
        """
        if not parsed_data:
            return parsed_data

        # Find the index of the last row with non-None cash_raised
        last_data_index = -1
        for i in range(len(parsed_data) - 1, -1, -1):
            if parsed_data[i]['cash_raised'] is not None:
                last_data_index = i
                break

        if last_data_index == -1:
            # No data found at all
            logger.warning("No rows with cash data found")
            return []

        # Include all rows up to and including the last row with data
        filtered_data = parsed_data[:last_data_index + 1]

        removed_count = len(parsed_data) - len(filtered_data)
        if removed_count > 0:
            logger.info(f"Filtered out {removed_count} trailing empty rows (future dates)")
            logger.debug(f"Last data date: {filtered_data[-1]['date']}")

        return filtered_data

    def create_data_file(self, parsed_data, output_path=None):
        """
        Create DATA Excel file in required format

        Format:
        Row 1: [blank], CODE_MNEMONIC
        Row 2: [blank], DESCRIPTION
        Row 3+: DATE, VALUE

        Args:
            parsed_data: List of {"date": str, "cash_raised": float/None}
            output_path: Optional custom output path

        Returns:
            str: Path to created file
        """
        if output_path is None:
            output_path = os.path.join(self.output_dir, config.DATA_FILENAME_PART1)

        logger.info(f"Creating DATA file: {os.path.basename(output_path)}")

        try:
            # Prepare data for DataFrame
            data_rows = []

            # Row 1: Headers (blank, code mnemonic)
            data_rows.append(['', config.OUTPUT_CODE_MNEMONIC])

            # Row 2: Descriptions (blank, description)
            data_rows.append(['', config.OUTPUT_DESCRIPTION])

            # Row 3+: Data rows (date, value)
            for row in parsed_data:
                date_str = row['date']
                cash_value = row['cash_raised']

                # Format cash value
                if cash_value is None:
                    cash_str = config.BLANK_VALUE_REPLACEMENT
                else:
                    # Format with appropriate decimal places
                    cash_str = round(cash_value, config.DECIMAL_PLACES)

                data_rows.append([date_str, cash_str])

            # Create DataFrame and write to Excel
            df = pd.DataFrame(data_rows)

            # Write to Excel without headers or index
            df.to_excel(output_path, index=False, header=False, engine='openpyxl')

            logger.info(f"[OK] DATA file created: {len(parsed_data)} rows")
            logger.debug(f"File path: {output_path}")

            return output_path

        except Exception as e:
            logger.error(f"Failed to create DATA file: {str(e)}")
            raise

    def create_meta_file(self, output_path=None):
        """
        Create META Excel file with metadata fields

        Format:
        Row 1: Column headers
        Row 2: Metadata values

        Args:
            output_path: Optional custom output path

        Returns:
            str: Path to created file
        """
        if output_path is None:
            output_path = os.path.join(self.output_dir, config.META_FILENAME_PART1)

        logger.info(f"Creating META file: {os.path.basename(output_path)}")

        try:
            # Prepare data
            meta_rows = []

            # Row 1: Column headers
            # Add empty first column to match reference format
            headers = [''] + config.METADATA_COLUMNS
            meta_rows.append(headers)

            # Row 2: Metadata values
            meta = config.METADATA_PART1
            values = [config.OUTPUT_CODE_MNEMONIC]  # First column is the full code

            # Add metadata values in order
            for col in config.METADATA_COLUMNS:
                values.append(meta.get(col, ''))

            meta_rows.append(values)

            # Create DataFrame and write to Excel
            df = pd.DataFrame(meta_rows)

            # Write to Excel without headers or index
            df.to_excel(output_path, index=False, header=False, engine='openpyxl')

            logger.info(f"[OK] META file created")
            logger.debug(f"File path: {output_path}")

            return output_path

        except Exception as e:
            logger.error(f"Failed to create META file: {str(e)}")
            raise

    def copy_to_latest(self, file_path):
        """
        Copy file to 'latest' folder

        Args:
            file_path: Path to file to copy

        Returns:
            str: Path to copied file in latest folder
        """
        filename = os.path.basename(file_path)
        latest_path = os.path.join(self.latest_dir, filename)

        try:
            shutil.copy2(file_path, latest_path)
            logger.debug(f"Copied to latest: {filename}")
            return latest_path

        except Exception as e:
            logger.warning(f"Could not copy to latest folder: {str(e)}")
            return None

    def generate_files(self, parsed_data):
        """
        Generate both DATA and META files and copy to latest folder

        Args:
            parsed_data: List of {"date": str, "cash_raised": float/None}

        Returns:
            dict: Result with file paths
                  {
                      "success": bool,
                      "data_file": str,
                      "meta_file": str,
                      "error": str,
                      "row_count": int
                  }
        """
        result = {
            "success": False,
            "data_file": None,
            "meta_file": None,
            "error": None,
            "row_count": 0
        }

        logger.info("Generating output files...")

        try:
            # Validate input
            if not parsed_data:
                result["error"] = "No data provided for file generation"
                logger.error(result["error"])
                return result

            # Filter out trailing empty rows (future dates with no data)
            filtered_data = self.filter_trailing_empty_rows(parsed_data)

            if not filtered_data:
                result["error"] = "No data rows with cash values found"
                logger.error(result["error"])
                return result

            result["row_count"] = len(filtered_data)

            # Create DATA file
            data_file = self.create_data_file(filtered_data)
            result["data_file"] = data_file

            # Create META file
            meta_file = self.create_meta_file()
            result["meta_file"] = meta_file

            # Copy to latest folder
            self.copy_to_latest(data_file)
            self.copy_to_latest(meta_file)

            logger.info("[OK] All output files generated successfully")

            # Summary
            logger.info("\nGenerated files:")
            logger.info(f"  DATA: {os.path.basename(data_file)}")
            logger.info(f"  META: {os.path.basename(meta_file)}")
            logger.info(f"\nFiles saved to:")
            logger.info(f"  Timestamped: {self.output_dir}")
            logger.info(f"  Latest:      {self.latest_dir}")

            result["success"] = True
            return result

        except Exception as e:
            result["error"] = str(e)
            logger.error(f"File generation failed: {str(e)}", exc_info=True)
            return result


def main():
    """
    Test function for file generator module
    """
    from logger_setup import log_section_header

    logger = setup_logger("generator_test")
    log_section_header(logger, "UKDMO FILE GENERATOR TEST")

    # Create sample test data
    test_data = [
        {"date": "2024-01-04", "cash_raised": 3308.36},
        {"date": "2024-01-09", "cash_raised": 2945.0968},
        {"date": "2024-01-10", "cash_raised": 4979.5421},
        {"date": "2024-01-16", "cash_raised": 1588.9866},
        {"date": "2024-01-17", "cash_raised": 3950.5832},
        {"date": "2024-01-31", "cash_raised": None},  # Blank value test
    ]

    logger.info(f"Test data: {len(test_data)} rows")

    # Create generator and generate files
    generator = UKDMOFileGenerator()
    result = generator.generate_files(test_data)

    # Print results
    logger.info("\n" + "=" * 70)
    logger.info("GENERATION RESULTS")
    logger.info("=" * 70)
    logger.info(f"Success: {result['success']}")
    logger.info(f"DATA file: {result['data_file']}")
    logger.info(f"META file: {result['meta_file']}")

    if result['error']:
        logger.error(f"Error: {result['error']}")

    # Display file contents (first few lines)
    if result['success'] and result['data_file']:
        logger.info("\nDATA file preview (first 10 lines):")
        try:
            with open(result['data_file'], 'r', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    if i < 10:
                        logger.info(f"  {line.rstrip()}")
        except Exception as e:
            logger.error(f"Could not read file: {e}")

        logger.info("\nMETA file contents:")
        try:
            with open(result['meta_file'], 'r', encoding='utf-8') as f:
                for line in f:
                    logger.info(f"  {line.rstrip()}")
        except Exception as e:
            logger.error(f"Could not read file: {e}")

    return result


if __name__ == "__main__":
    main()
