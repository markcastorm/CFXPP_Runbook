# CFXPP Comparison Tool

Powerful standalone tool to compare pipeline output against manual reference files.

## Quick Start

1. **Edit `config.py`** — Set paths to your files:
   ```python
   OUTPUT_DATA = r'path\to\CFXPP_DATA_20260327_093125.xlsx'
   REFERENCE_DATA = r'path\to\reference\CFXPP_DATA_20260324.xlsx'
   MAPPING_CSV = r'path\to\source_column_mapping.csv'
   ARCHIVE_DIR = r'path\to\archive\20260320_20260326'
   ```

2. **Run comparison:**
   ```bash
   cd D:\Projects\SIMBA-RUNBOOKS\CFXPP_Runbook\compair
   python compare.py
   ```

3. **Review reports** in the timestamped `run_YYYYMMDD_HHMMSS/` folder:
   - `comparison_report.csv` — Detailed row-by-row comparison
   - `comparison_summary.txt` — Executive summary with statistics
   - `ANNOTATED_Pipeline_*.xlsx` — Pipeline output with mismatches highlighted in RED
   - `ANNOTATED_Reference_*.xlsx` — Reference file with mismatches highlighted in RED, missing in YELLOW
   - `ORIGINAL_*.xlsx` — Copies of both original files

## Output Structure

Each run creates a timestamped folder: `run_20260327_141500/`

```
compair/
├── run_20260327_141500/
│   ├── comparison_report.csv
│   ├── comparison_summary.txt
│   ├── ANNOTATED_Pipeline_CFXPP_DATA_20260327_093125.xlsx  ← Mismatches in RED
│   ├── ANNOTATED_Reference_CFXPP_DATA_20260324.xlsx       ← Mismatches in RED, Missing in YELLOW
│   ├── ORIGINAL_CFXPP_DATA_20260327_093125.xlsx
│   └── ORIGINAL_CFXPP_DATA_20260324.xlsx
├── run_20260327_153000/
│   └── ...
```

## Output Reports

### comparison_report.csv

Detailed CSV with one row per discrepancy:

| Column | Description |
|--------|-------------|
| Status | MISMATCH, MISSING, or EXTRA |
| Date | Date of the cell |
| Output_Column | Column index in Excel |
| Column_Code | Full CFXPP column code |
| Section | G10 CCY / EM CCY / FX PAIR |
| Currency_Pair | Currency pair or group/currency |
| Client_Type | BANKS, BROKER, etc. |
| Metric | Volume, Price, or Positioning |
| Column_Description | Human-readable description |
| Pipeline_Value | Value from pipeline output |
| Reference_Value | Value from reference file |
| Difference | Numeric difference (pipeline - reference) |
| Source_File(s) | Source Excel files |
| Archive_Link(s) | **Clickable paths to archived files** |
| Raw_Client_Types | Original client type strings |

### ANNOTATED Excel Files

**Visual comparison** with color-coded highlighting:

| Color | Meaning | In Pipeline File | In Reference File |
|-------|---------|------------------|-------------------|
| 🔴 RED | Mismatch - values differ | ✓ | ✓ |
| 🟡 YELLOW | Missing - ref has, pipeline doesn't | | ✓ |
| 🟢 GREEN | Extra - pipeline has, ref doesn't | ✓ | |

**Legend row** inserted at top of each file showing color meanings.

**Quick workflow:**
1. Open `ANNOTATED_Pipeline_*.xlsx`
2. Scroll down to find RED cells
3. Check corresponding cells in reference file
4. Click archive link in CSV report to open source file

### comparison_summary.txt

Human-readable text summary:
- Overall match statistics
- Mismatches by section
- Top mismatches by magnitude
- Missing cells by currency pair
- Extra cells by currency pair

## Configuration Options

Edit `config.py` to customize:

```python
# Floating point tolerance (default: 0.001)
FLOAT_TOLERANCE = 0.001

# Show detailed progress (default: True)
VERBOSE = True

# Number of examples in console output (default: 5)
MAX_CONSOLE_EXAMPLES = 5

# Custom report output path (default: auto-generate)
REPORT_OUTPUT = None
```

## Understanding the Reports

### Status Types

- **MISMATCH**: Both files have values but they differ
- **MISSING**: Reference has value, pipeline doesn't (need source files)
- **EXTRA**: Pipeline has value, reference doesn't (unexpected data)

### Common Patterns

1. **Small float differences** (< 0.001) — Rounding, counted as matches
2. **Missing with "NO SOURCE FILE"** — Need to add source files for those pairs
3. **Large mismatches** — Usually reference errors or duplicate files
4. **Multiple source files** — Check for duplicate extractions

### Archive Links

Click the `Archive_Link(s)` cell in Excel to open the source file directly from the archive.

## Workflow

1. Run pipeline: `python orchestrator.py`
2. Update paths in `compair/config.py`
3. Run comparison: `python compare.py`
4. Review `comparison_report.csv` for discrepancies
5. Investigate mismatches using archive links
6. Verify missing cells against expected coverage

## Example Output

```
======================================================================
CFXPP COMPARISON TOOL
======================================================================

Loading Pipeline Output: CFXPP_DATA_20260327_093125.xlsx
  Loaded: 540 codes, 1716 cells
Loading Reference: CFXPP_DATA_20260324 (1).xlsx
  Loaded: 540 codes, 1977 cells
Loading source mapping: source_column_mapping.csv
  Loaded: 540 column mappings
Loading META descriptions: CFXPP_META_20260327_093125.xlsx
  Loaded: 540 descriptions

Performing comparison...
  Total cell locations: 2238
  Matches:    1716
  Mismatches: 33
  Missing:    228
  Extra:      0

Writing CSV report: comparison_report.csv
  Written 261 rows

Writing summary report: comparison_summary.txt
  Summary written

======================================================================
COMPARISON COMPLETE
======================================================================

Matches:    1,716 (76.68%)
Mismatches: 33
Missing:    228
Extra:      0

Reports saved:
  CSV:     D:\...\output\20260327_093125\comparison_report.csv
  Summary: D:\...\output\20260327_093125\comparison_summary.txt
======================================================================
```
