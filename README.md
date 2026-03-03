# UKDMOGI Data Scraper

**UK Debt Management Office - Gilt Issuance Data Pipeline**

Automated data extraction and processing pipeline for UK government debt management data.

---

## Overview

This project automates the collection and processing of gilt market data from the UK Debt Management Office (DMO) website. It scrapes, parses, and generates standardized CSV files for financial analysis.

### Part 1: Outright Gilt Issuance Calendar
- **Source**: https://www.dmo.gov.uk/data/pdfdatareport?reportCode=D5D
- **Data Extracted**: Operation Date, Cash Raised (£ million)
- **Frequency**: Business day (B)

---

## Architecture

This project follows the proven **modular pipeline architecture** from the CHEF_NOVARTIS project:

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   SCRAPER   │ ───> │   PARSER    │ ───> │  GENERATOR  │ ───> │   OUTPUT    │
│ (Download)  │      │  (Extract)  │      │  (Transform)│      │   (Files)   │
└─────────────┘      └─────────────┘      └─────────────┘      └─────────────┘
      ▲                                                                │
      │                                                                │
      └────────────────────── ORCHESTRATOR ───────────────────────────┘
                            (Workflow Coordinator)
```

### Components

1. **config.py** - Centralized configuration management
   - URLs, selectors, metadata
   - Column mappings and formatting rules
   - Directory structure and file naming

2. **logger_setup.py** - Comprehensive logging infrastructure
   - Dual handlers (console + file)
   - Timestamped log files
   - Debug mode support

3. **scraper.py** - Web automation (Selenium)
   - Cookie consent handling
   - Form interaction and data selection
   - Excel file download

4. **parser.py** - Data extraction and transformation
   - Excel file parsing (.xls format)
   - Date parsing and formatting
   - Value extraction and validation

5. **file_generator.py** - Output file generation
   - DATA CSV (time series data)
   - META CSV (metadata)
   - Timestamped folders with "latest" copy

6. **orchestrator.py** - Main workflow coordinator
   - Sequential stage execution
   - Error handling and logging
   - Summary reporting

---

## Project Structure

```
UKDMOGI_DATA_PART_1/
├── config.py                 # Configuration settings
├── logger_setup.py          # Logging infrastructure
├── scraper.py               # Web scraper
├── parser.py                # Data parser
├── file_generator.py        # Output generator
├── orchestrator.py          # Main coordinator
├── requirements.txt         # Python dependencies
├── README.md                # This file
│
├── downloads/               # Downloaded Excel files (timestamped)
│   ├── 20250113_143022/
│   └── ...
│
├── output/                  # Generated CSV files
│   ├── 20250113_143022/
│   │   ├── UKDMOGI_DATA_PART_1_20250113.csv
│   │   └── UKDMOGI_META_PART_1_20250113.csv
│   └── latest/              # Always contains most recent files
│
├── logs/                    # Execution logs (timestamped)
│   ├── 20250113_143022/
│   └── ...
│
└── bin/                     # Test scripts (optional)
```

---

## Installation

### Prerequisites

1. **Python 3.8+**
2. **Google Chrome** or Chromium browser
3. **ChromeDriver** (matching Chrome version)

### Setup Steps

1. **Clone or download the project**

2. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Install ChromeDriver**

   **Windows (with Chocolatey):**
   ```bash
   choco install chromedriver
   ```

   **Mac (with Homebrew):**
   ```bash
   brew install chromedriver
   ```

   **Linux:**
   ```bash
   apt-get install chromium-chromedriver
   ```

   **Manual installation:**
   - Download from: https://chromedriver.chromium.org/
   - Add to system PATH

4. **Verify installation**
   ```bash
   python -c "import selenium; print('Selenium:', selenium.__version__)"
   ```

---

## Usage

### Basic Usage (Latest Financial Year)

```bash
python orchestrator.py
```

This will:
1. Download data for the latest financial year
2. Parse the Excel file
3. Generate DATA and META CSV files
4. Save to timestamped and "latest" folders

### Specify Financial Year

```bash
python orchestrator.py 2024-25
```

Available financial years: 1998-99 through 2025-26 (or current year)

### Configuration Options

Edit `config.py` to customize:

```python
# Default financial year selection
TARGET_FINANCIAL_YEAR = None  # None = latest, or "2024-25" for specific

# Browser mode
HEADLESS_MODE = False  # True for background operation (no browser window)

# Debug mode
DEBUG_MODE = True  # Detailed logging in file
```

---

## Output Files

### DATA File Format

```csv
,UKDMOGI.CASHRAISED.B
,Cash Raised
2024-01-04,3308.36
2024-01-09,2945.0968
2024-01-10,4979.5421
...
```

- **Row 1**: Code mnemonic header
- **Row 2**: Description header
- **Row 3+**: Date, Value pairs

### META File Format

```csv
,CODE_MNEMONIC,DESCRIPTION,FREQUENCY,MULTIPLIER,AGGREGATION_TYPE,...
UKDMOGI.CASHRAISED.B,UKDMOGI.CASHRAISED,Cash Raised,B,6,UNDEFINED,...
```

Contains metadata fields:
- CODE_MNEMONIC, DESCRIPTION, FREQUENCY
- MULTIPLIER, AGGREGATION_TYPE, UNIT_TYPE
- DATA_TYPE, DATA_UNIT, SEASONALLY_ADJUSTED
- PROVIDER, SOURCE, COUNTRY, DATASET

---

## Testing Individual Modules

Each module can be tested independently:

### Test Config
```bash
python config.py
```

### Test Logger
```bash
python logger_setup.py
```

### Test Scraper
```bash
python scraper.py
```

### Test Parser
```bash
python parser.py
```

### Test File Generator
```bash
python file_generator.py
```

---

## Logging

### Console Output
- INFO level: User-friendly progress messages
- Colored indicators: ✓ (success), ✗ (error), ⚠ (warning)

### Log Files
- Location: `logs/<timestamp>/ukdmogi_<timestamp>.log`
- DEBUG level: Detailed troubleshooting information
- Includes full stack traces for errors

---

## Error Handling

The pipeline includes comprehensive error handling:

1. **Stage-level**: Each stage validates its results
2. **Graceful degradation**: Continues when possible (configurable)
3. **Detailed logging**: All errors logged with context
4. **Exit codes**:
   - `0` = Success
   - `1` = Failure
   - `130` = User interrupted (CTRL+C)

---

## Configuration Reference

### Key Settings in config.py

| Setting | Default | Description |
|---------|---------|-------------|
| `TARGET_FINANCIAL_YEAR` | `None` | Financial year to scrape (None = latest) |
| `HEADLESS_MODE` | `False` | Run browser in background |
| `DEBUG_MODE` | `True` | Enable detailed logging |
| `MIN_DATA_ROWS` | `1` | Minimum rows required for valid data |
| `COOKIE_WAIT_TIMEOUT` | `10` | Seconds to wait for cookie banner |
| `DOWNLOAD_WAIT_TIMEOUT` | `60` | Seconds to wait for file download |
| `DECIMAL_PLACES` | `4` | Number of decimal places for values |

---

## Troubleshooting

### Issue: ChromeDriver not found
**Solution**: Install ChromeDriver and add to PATH

### Issue: Cookie banner not appearing
**Solution**: This is normal if cookies already accepted. Pipeline continues.

### Issue: Download timeout
**Solution**: Increase `DOWNLOAD_WAIT_TIMEOUT` in config.py

### Issue: Excel parsing errors
**Solution**: Check log file for details. Verify Excel file format hasn't changed.

### Issue: Browser window closes immediately
**Solution**: Check logs for errors. Verify Selenium is installed correctly.

---

## Architecture Highlights

### Design Patterns Used

1. **Pipeline Pattern**: Sequential data flow through stages
2. **Orchestrator Pattern**: Central coordinator manages workflow
3. **Configuration Pattern**: Centralized settings management
4. **Factory Pattern**: Consistent output file generation

### Best Practices

- ✓ Modular design (separation of concerns)
- ✓ Configuration-driven (no hardcoding)
- ✓ Comprehensive logging
- ✓ Error handling at every stage
- ✓ Timestamped organization
- ✓ "Latest" folder for quick access
- ✓ Graceful shutdown (CTRL+C handling)

---

## Future Enhancements (Part 2)

Part 2 will handle:
- **Source**: Redemption Details of Redeemed Gilts
- **URL**: https://www.dmo.gov.uk/data/ExportReport?reportCode=D1C
- **Data**: Nominal amount outstanding (negative values)
- **Special logic**: Weekend consolidation, value aggregation

Configuration placeholders already exist in `config.py`.

---

## Credits

**Architecture based on**: CHEF_NOVARTIS project
**Provider**: AfricaAI
**Source**: UK Debt Management Office (UKDMO)
**Country**: United Kingdom (GBR)

---

## License

Internal use only. Data sourced from UK Debt Management Office public website.

---

## Support

For issues or questions:
1. Check log files: `logs/<timestamp>/ukdmogi_<timestamp>.log`
2. Review configuration: `config.py`
3. Test individual modules (see Testing section)

---

**Last Updated**: 2025-01-13
**Version**: 1.0.0 (Part 1 only)
