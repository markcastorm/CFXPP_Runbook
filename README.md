# CFXPP Runbook

**Barclays FX Pair Positioning Data Processing Pipeline**

High-performance, multiprocessing Python pipeline for processing Barclays FX export files into standardized DATA + META + ZIP outputs with 540 data columns.

## Overview

Processes ~250 Excel export files from Barclays into structured output files suitable for Bloomberg upload. Built as an enhanced version of the JPMRGDPF pipeline with improved performance, validation, and reporting capabilities.

### Key Features

- **Multiprocessing**: 4-worker parallel processing for fast execution
- **540 Output Columns**: Comprehensive FX positioning data across 21 currency pairs and 9 client types
- **Incremental Updates**: Merge new data into existing master files
- **Source Tracking**: Detailed mapping from source files to output columns with archive links
- **Validation**: Built-in comparison tool for verifying output against reference data
- **Coverage Reporting**: Real-time tracking of filled vs. missing columns
- **Auto-Archiving**: Processed files automatically moved to batch-specific archive folders

## Pipeline Performance

**Latest Run Results:**
- 243 files processed in ~13 seconds
- 491/540 columns filled (90.9% coverage)
- 2,197 data cells across 5 dates
- 97.59% match rate against reference data

## Quick Start

### Prerequisites

```bash
pip install -r requirements.txt
```

**Required packages:**
- openpyxl
- pandas
- python-dateutil

### Basic Usage

1. **Drop files** into the `Input/` directory (nested subfolders supported)

2. **Run the pipeline:**
   ```bash
   python orchestrator.py
   ```

3. **Review outputs:**
   - `output/latest/` — Latest DATA, META, and ZIP files
   - `output/{timestamp}/` — Timestamped output with reports
   - `Master Data/` — Rolling master file per batch
   - `archive/{batch_id}/` — Archived processed files

4. **Validate results:**
   ```bash
   cd compair
   # Edit config.py with file paths
   python compare.py
   ```

## Output Files

### DATA File
Excel file with 541 columns (1 date + 540 data):
- **Rows**: One per date in batch range
- **Column 1**: Date (YYYY-MM-DD)
- **Columns 2-541**: Positioning data for each FX pair/client/metric combination

### META File
Excel file with metadata for each column:
- Column codes (e.g., `CFXPP.FXPAIRPOSITIONING...BANKS.VOLUME_NORMALIZED.EURUSD.D`)
- Human-readable descriptions
- Section classifications

### ZIP File
Compressed archive containing both DATA and META files for easy distribution.

## Output Column Structure

**540 data columns organized in 3 sections:**

### Section A: G10 Currency Positioning (90 columns)
- Columns 2-91
- 9 client types × 10 G10 currencies
- Example: `CFXPP.CURRENCYPOSITIONING.OVERVIEWOFCUMULATIVEPOSITIONS.POSITIONS.BANKS.G10.USD.B`

### Section B: EM Currency Positioning (72 columns)
- Columns 92-163
- 9 client types × 8 EM currencies
- Example: `CFXPP.CURRENCYPOSITIONING.OVERVIEWOFCUMULATIVEPOSITIONS.BANKS.EM.TRY.B`

### Section C: FX Pair Positioning (378 columns)
- Columns 164-541
- 21 pairs × 9 client types × 2 metrics (Volume + Price)
- Example: `CFXPP.FXPAIRPOSITIONING.NETCUMULATIVEPOSITIONSOFCURRENCYPAIRS.BANKS.VOLUME_NORMALIZED.EURUSD.D`

## Source File Types

### Type 1: FX Pair Positioning (Majority)
- **Identifies by**: "Currency Pair" label in header
- **Contains**: Date, Time, Volume (normalized), Closing Price
- **Maps to**: Columns 164-541 (378 columns)

### Type 2: Currency Positioning (Fewer files)
- **Identifies by**: "Ccy Group" label in header
- **Contains**: Currency code, Net Cumulative Positioning (normalized)
- **Maps to**: Columns 2-163 (162 columns)

## Client Type Mappings (9 Types)

| Source String | Code | Description |
|--------------|------|-------------|
| "Banks, Broker, Corporate, Hedge Fund, Real Money, Unclassified" | `ALL` | All client types combined |
| "Banks, Broker" | `BANKS_BROKER` | Banks + Brokers |
| "Corporate, Real Money" | `CORPORATE_REALMONEY` | Corporate + Real Money |
| "Banks" | `BANKS` | Banks only |
| "Broker" | `BROKER` | Brokers only |
| "Corporate" | `CORPORATE` | Corporate only |
| "Hedge Fund" | `HEDGEFUND` | Hedge funds only |
| "Real Money" | `REALMONEY` | Real money investors only |
| "Unclassified" | `UNCLASSIFIED` | Unclassified clients |

**Note:** Files with unrecognized client types (e.g., "Hedge Fund, Broker") are automatically skipped as noise.

## Currency Pairs Supported (21 Pairs)

**G10 Majors (7):**
- EURUSD, GBPUSD, AUDUSD, USDJPY, CHFUSD, USDCAD, NZDUSD

**Emerging Markets (14):**
- USDBRL, USDCNH, USDCLP, USDCOP, USDHKD, USDINR
- USDKRW, USDMXN, USDPEN, USDPHP, USDPLN, USDSGD
- USDTRY, USDTWD, USDZAR

## Pipeline Architecture

### Core Modules (9)

| Module | Purpose |
|--------|---------|
| `config.py` | Central configuration, client mappings, currency orders |
| `logger_setup.py` | Timestamped logging with console output |
| `column_mapper.py` | Generates 540 column codes with validation |
| `data_loader.py` | Recursive file scanner for Input/ directory |
| `parser.py` | Dynamic parser for both file types, multiprocessing-safe |
| `file_generator.py` | DATA + META + ZIP generation, master management |
| `tracker.py` | Coverage tracking, source mapping, skipped files reporting |
| `archiver.py` | Batch-specific archiving of processed files |
| `orchestrator.py` | 10-step pipeline with multiprocessing |

### Pipeline Flow (10 Steps)

1. **Scan Input** — Discover all Excel files (nested folders OK)
2. **Parse Files** — Extract data with 4-worker multiprocessing
3. **Identify Batch** — Determine date range and batch ID
4. **Load Master** — Check for existing master data to merge
5. **Map Columns** — Match parsed data to 540 output columns
6. **Build DataFrame** — Construct output with all dates in range
7. **Generate Files** — Create DATA, META, ZIP in timestamped + latest folders
8. **Save Reports** — Coverage, mapping, and skipped files reports
9. **Archive Files** — Move processed files to `archive/{batch_id}/`
10. **Summary** — Display execution statistics

## Reports Generated

### coverage_report.csv
Tracks which columns are filled vs. empty:
- Column code, description, section
- Fill status (FILLED/EMPTY)
- Number of dates with data

### source_column_mapping.csv
**Detailed mapping from source files to output columns:**
- Source file name and type
- Raw and normalized client types
- Currency pair and date range
- Output column code and index
- Actual values by date
- Duplicate detection
- **Clickable archive links** to source files

### skipped_files.csv
Lists files that were parsed but skipped (unknown client types).

### processed_files.csv
Simple list of all successfully processed files with metadata.

## Comparison Tool

Powerful standalone tool in `compair/` directory for validating pipeline output against reference data.

### Features
- Cell-by-cell comparison with 0.001 float tolerance
- Color-coded Excel highlighting (RED=mismatch, YELLOW=missing, GREEN=extra)
- Timestamped output folders with full history
- CSV reports with clickable archive links
- Executive summary with statistics

### Usage

1. Edit `compair/config.py` with file paths:
   ```python
   OUTPUT_DATA = r'path\to\CFXPP_DATA_20260327_152850.xlsx'
   REFERENCE_DATA = r'path\to\reference\CFXPP_DATA_20260324.xlsx'
   MAPPING_CSV = r'path\to\source_column_mapping.csv'
   ARCHIVE_DIR = r'path\to\archive\20260323_20260327'
   ```

2. Run comparison:
   ```bash
   cd compair
   python compare.py
   ```

3. Review timestamped `run_YYYYMMDD_HHMMSS/` folder:
   - `comparison_report.csv` — Detailed row-by-row comparison
   - `comparison_summary.txt` — Executive summary
   - `ANNOTATED_Pipeline_*.xlsx` — Output with RED/GREEN highlighting
   - `ANNOTATED_Reference_*.xlsx` — Reference with RED/YELLOW highlighting
   - `ORIGINAL_*.xlsx` — Copies of both files

See [compair/README.md](compair/README.md) for detailed documentation.

## Configuration

Edit `config.py` to customize:

```python
# Directories
INPUT_DIR = r'D:\Projects\SIMBA-RUNBOOKS\CFXPP_Runbook\Input'
OUTPUT_DIR = r'D:\Projects\SIMBA-RUNBOOKS\CFXPP_Runbook\output'
MASTER_DIR = r'D:\Projects\SIMBA-RUNBOOKS\CFXPP_Runbook\Master Data'
ARCHIVE_DIR = r'D:\Projects\SIMBA-RUNBOOKS\CFXPP_Runbook\archive'

# Processing
MAX_WORKERS = 4  # Multiprocessing worker count

# Currency pairs (21 supported)
FX_PAIRS = ['EURUSD', 'GBPUSD', 'AUDUSD', ...]

# Client type mappings (9 types)
CLIENT_TYPE_MAP = {
    "Banks, Broker, Corporate, Hedge Fund, Real Money, Unclassified": "ALL",
    ...
}
```

## Technical Details

### Critical Implementation Notes

- **openpyxl**: Must use `data_only=True` without `read_only=True` (Barclays files fail with read_only)
- **Column Codes**: Validated 540/540 perfect match against reference — do NOT modify generation logic
- **G10 vs EM**: G10 has `.POSITIONS.` in path; EM does not (intentional)
- **Known Typo**: `BANKS_BROKERVOLUME_NORMALIZED` (missing dot) — replicated from reference
- **Currency Orders**: Each client type has its OWN unique order for G10 and EM currencies
- **Parsing**: `parse_single_file()` must be module-level for ProcessPoolExecutor pickling
- **Date Ranges**: Includes ALL dates in range (weekends too) with blanks where no data

### Batch ID Format
`{STARTDATE}_{ENDDATE}` (e.g., `20260323_20260327`)

### Master File Management
- One master file per batch: `Master_CFXPP_DATA_{batch_id}.xlsx`
- New data merges into existing master if found
- Supports incremental updates (drop more files, run again)

## Directory Structure

```
CFXPP_Runbook/
├── orchestrator.py          # Main pipeline entry point
├── config.py                # Central configuration
├── column_mapper.py         # Column code generation
├── data_loader.py           # File discovery
├── parser.py                # Excel file parsing
├── file_generator.py        # Output file creation
├── tracker.py               # Coverage and mapping tracking
├── archiver.py              # File archiving
├── logger_setup.py          # Logging setup
├── requirements.txt         # Python dependencies
├── README.md                # This file
├── CLAUDE.md                # AI assistant context
├── .gitignore               # Git ignore rules
│
├── Input/                   # Drop source files here (nested OK)
│
├── output/                  # Pipeline outputs
│   ├── latest/              # Latest DATA, META, ZIP
│   └── {timestamp}/         # Timestamped outputs with reports
│
├── Master Data/             # Rolling master files per batch
│
├── archive/                 # Processed files by batch
│   └── {batch_id}/          # One folder per batch
│
├── logs/                    # Timestamped + latest logs
│   ├── latest/
│   └── {timestamp}/
│
├── compair/                 # Comparison validation tool
│   ├── compare.py           # Main comparison script
│   ├── config.py            # Comparison configuration
│   ├── README.md            # Comparison tool docs
│   ├── run_comparison.bat  # Windows launcher
│   └── run_{timestamp}/    # Timestamped comparison results
│
└── Project information/     # Reference docs and notes
    ├── CFXPP_DATA_20260324.xlsx
    ├── information.txt
    └── image1-5.png
```

## Troubleshooting

### Common Issues

**1. "No module named 'openpyxl'"**
```bash
pip install -r requirements.txt
```

**2. "PermissionError: [Errno 13]"**
- Close Excel if files are open
- Check that output directories are not read-only

**3. "Unknown client type: Hedge Fund, Broker"**
- This is expected — these files are noise from Barclays export tool
- Pipeline automatically skips them
- Check `skipped_files.csv` for details

**4. "No files found in Input/"**
- Check that files are .xlsx format
- Verify INPUT_DIR path in config.py
- Pipeline supports nested subfolders

**5. Low coverage percentage**
- Missing source files for certain currency pair/client combinations
- Check `coverage_report.csv` for which columns are empty
- Review `MISSING CELLS BY CURRENCY PAIR` in comparison summary

## Performance Benchmarks

**Test Run (2026-03-27):**
- Files processed: 243 (169 FX + 74 CCY)
- Processing time: ~13 seconds
- Output: 5 rows × 541 columns
- Coverage: 491/540 columns (90.9%)
- Cells filled: 2,197/2,700 (81.4%)
- Workers: 4 parallel processes

## Data Validation

The pipeline includes comprehensive validation:

1. **Column Code Validation**: 540/540 codes match reference exactly
2. **Client Type Validation**: Maps 9 known types, skips unknown
3. **Date Range Validation**: Ensures continuous date coverage
4. **Coverage Tracking**: Reports filled vs. missing columns
5. **Source Mapping**: Traces every cell back to source file
6. **Comparison Tool**: Validates output against reference data

## Version History

### Current Version
- **Status**: Production-ready, fully tested
- **Coverage**: 90.9% with 243 source files
- **Validation**: 97.59% match rate against reference
- **Performance**: 13 seconds for 243 files with 4 workers

### Recent Enhancements
- Comparison tool with color-coded Excel highlighting
- Source-to-column mapping with archive links
- Skipped files reporting
- Incremental batch updates
- Multiprocessing optimization

## Author

**Mark Castro**

## License

Internal use only. Confidential and proprietary.

## Related Projects

- **JPMRGDPF Pipeline**: Base pipeline from which CFXPP was enhanced
  - Location: `D:\Projects\SIMBA-RUNBOOKS\JPMRGDPF_Runbook\`

## Support

For issues or questions:
1. Check this README and `compair/README.md`
2. Review log files in `logs/latest/`
3. Check `coverage_report.csv` for missing data
4. Use comparison tool to validate output
5. Review `skipped_files.csv` for parsing issues

---

**Last Updated**: 2026-03-27
