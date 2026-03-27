# CFXPP Runbook — Project Context

## What This Is
Barclays FX Pair Positioning data processing pipeline. Processes ~248+ Excel export files from Barclays into standardized DATA + META + ZIP output files with 541 columns (540 data + 1 date). Built as an enhanced version of the JPMRGDPF pipeline (`D:\Projects\SIMBA-RUNBOOKS\JPMRGDPF_Runbook\`).

## Current Status: PRODUCTION-READY & VALIDATED
- All 9 modules built, tested, and working flawlessly
- Column codes validated **540/540 perfect match** against reference output
- Latest test: 243 files processed → 491/540 columns filled (90.9% coverage), 2,197 cells
- **97.59% validation match rate** against reference data (only 8 mismatches out of 2,238 cells)
- Incremental batch updates working (second run merges into existing master)
- Comprehensive comparison tool with color-coded Excel highlighting and archive tracing
- Source-to-column mapping with clickable archive links for instant diagnosis
- Files with unknown client types (e.g., "Hedge Fund, Broker") are gracefully skipped — these are noise from Barclays export tool, not part of the 9 expected client types

## Architecture (9 modules)

| Module | Purpose |
|--------|---------|
| `config.py` | Central config: paths, 9 client type mappings, 21 FX pairs, per-client currency orders, metadata |
| `logger_setup.py` | Timestamped + latest log files with console output |
| `column_mapper.py` | Generates exact 540 column codes with validation against reference |
| `data_loader.py` | Recursive file scanner for Input/ with nested subfolders |
| `parser.py` | Dynamic parser for 2 file types (FX Pair + Currency Positioning), multiprocessing-safe |
| `file_generator.py` | DATA + META + ZIP generation, timestamped + latest folders, master management |
| `tracker.py` | Coverage tracking, source-to-column mapping with archive links, skipped files reporting |
| `archiver.py` | Moves processed files to `archive/{batch_id}/` |
| `orchestrator.py` | 10-step pipeline with ProcessPoolExecutor multiprocessing |

## Two Source File Types

### Type 1: FX Pair Positioning (majority of files)
- Has: Currency Pair, Client Types, STARTDATE, ENDDATE, Interval
- Data: Date, Time, Volume (normalized), Closing Price
- Maps to output cols 164-541 (378 columns)
- Identifying label: `Currency Pair`

### Type 2: Currency Positioning (fewer files)
- Has: Client Types, STARTDATE, Ccy Group (G10/EM)
- Data: Currency code, Net Cumulative Positioning (normalized)
- Maps to output cols 2-163 (162 columns)
- Identifying label: `Ccy Group`

## 9 Client Type Combinations
```
"Banks, Broker, Corporate, Hedge Fund, Real Money, Unclassified" → ALL
"Banks, Broker" → BANKS_BROKER
"Corporate, Real Money" → CORPORATE_REALMONEY
"Banks" → BANKS
"Broker" → BROKER
"Corporate" → CORPORATE
"Hedge Fund" → HEDGEFUND
"Real Money" → REALMONEY
"Unclassified" → UNCLASSIFIED
```

## Output Column Structure (540 data columns)

### Section A: G10 Currency Positioning (cols 2-91, 90 columns)
- Code: `CFXPP.CURRENCYPOSITIONING.OVERVIEWOFCUMULATIVEPOSITIONS.POSITIONS.{CLIENT}.G10.{CCY}.B`
- Note: includes `.POSITIONS.` segment
- 9 client types × 10 G10 currencies, each client type has its OWN unique currency order

### Section B: EM Currency Positioning (cols 92-163, 72 columns)
- Code: `CFXPP.CURRENCYPOSITIONING.OVERVIEWOFCUMULATIVEPOSITIONS.{CLIENT}.EM.{CCY}.B`
- Note: NO `.POSITIONS.` segment (different from G10)
- 9 client types × 8 EM currencies, each client type has its OWN unique currency order

### Section C: FX Pair Positioning (cols 164-541, 378 columns)
- Code: `CFXPP.FXPAIRPOSITIONING.NETCUMULATIVEPOSITIONSOFCURRENCYPAIRS.{CLIENT}.{METRIC}.{PAIR}.D`
- 21 pairs × 9 client types × 2 metrics (VOLUME_NORMALIZED, CLOSING_PRICE) = 378
- **KNOWN TYPO** (replicated intentionally): BANKS_BROKER + VOLUME_NORMALIZED = `BANKS_BROKERVOLUME_NORMALIZED` (missing dot)
- **EM BANKS_BROKER description anomaly**: uses "Banks, Broker, Corporate" instead of "Banks, Broker"

## Key Technical Decisions
- `read_only=True` does NOT work for these Barclays files — must use `data_only=True` without `read_only`
- Parser is fully dynamic — scans cells for labels, never hardcodes row/col positions
- `parse_single_file()` is module-level (not a method) for ProcessPoolExecutor pickling
- Batch ID format: `{STARTDATE}_{ENDDATE}` (e.g., `20260320_20260325`)
- Date rows include ALL dates in range (including weekends) — blanks where no data
- Files with unrecognized client types (e.g., "Hedge Fund, Broker") are skipped as noise

## Pipeline Usage
```bash
# Drop files into Input/ (supports nested subfolders), then:
python orchestrator.py
# Output → output/{timestamp}/ and output/latest/
# Master → Master Data/Master_CFXPP_DATA_{batch_id}.xlsx
# Archive → archive/{batch_id}/
# Reports → output/{timestamp}/ (coverage, mapping, skipped files)
```

## Reports Generated

### coverage_report.csv
Tracks filled vs. empty columns with section breakdown.

### source_column_mapping.csv (CRITICAL for diagnosis)
**Detailed mapping from every source file to every output column:**
- Shows which source file(s) filled which output columns
- Includes actual date=value pairs for each mapping
- **Clickable archive links** to instantly open source files
- Detects duplicates (multiple files mapping to same column)
- Lists raw client type strings from source files
- One row per (source_file, output_column) combination

### skipped_files.csv
Files that were parsed but skipped due to unknown client types (e.g., "Broker, Hedge Fund").

### processed_files.csv
Simple list of all successfully processed files with metadata.

## Comparison Tool (compair/)

**Standalone validation tool for comparing pipeline output against reference data.**

**Location:** `D:\Projects\SIMBA-RUNBOOKS\CFXPP_Runbook\compair\`

**Usage:**
1. Edit `compair/config.py` with file paths (OUTPUT_DATA, REFERENCE_DATA, MAPPING_CSV, ARCHIVE_DIR)
2. Run: `python compare.py` (or `run_comparison.bat` on Windows)
3. Review timestamped `run_{timestamp}/` folder

**Features:**
- Cell-by-cell comparison with 0.001 float tolerance
- **Color-coded Excel highlighting:**
  - RED = Mismatches (values differ)
  - YELLOW = Missing (reference has, pipeline doesn't)
  - GREEN = Extra (pipeline has, reference doesn't)
- Timestamped output folders with full history
- CSV reports with **clickable archive links** for instant source file access
- Executive summary with statistics and top mismatches
- Copies of both original files for reference
- Legend row inserted at top of annotated Excel files

**Typical Results:**
- Latest run: 97.59% match rate (2,184 matches, 8 mismatches, 41 missing, 5 extra)
- Most "mismatches" are actually reference file errors (proven in previous validation where all 122 initial mismatches were reference errors)

See `compair/README.md` for detailed documentation.

## Reference Files
- Manual reference output: `Project information/CFXPP_DATA_20260324.xlsx` (used for validation)
- Project notes: `Project information/information.txt`
- Reference images: `Project information/image1-5.png`

## Directory Structure
```
CFXPP_Runbook/
├── orchestrator.py, config.py, data_loader.py, parser.py
├── column_mapper.py, file_generator.py, tracker.py, archiver.py
├── logger_setup.py, requirements.txt, README.md, CLAUDE.md, .gitignore
├── Input/                # Drop source files here (nested subfolders OK)
├── output/               # Timestamped + latest output with reports
│   └── {timestamp}/      # coverage_report.csv, source_column_mapping.csv, skipped_files.csv
├── archive/              # Processed files by batch
├── Master Data/          # Rolling master per batch
├── logs/                 # Timestamped + latest logs
├── compair/              # Standalone comparison tool
│   ├── compare.py        # Main comparison script
│   ├── config.py         # Comparison configuration
│   ├── README.md         # Comparison tool documentation
│   ├── run_comparison.bat
│   └── run_{timestamp}/  # Timestamped comparison results with annotated Excel
└── Project information/  # Reference docs
```
