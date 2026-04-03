# Source Coverage Verification Tool

Verifies pipeline output against archived source files to ensure all data is accounted for.

## What It Does

1. **Scans Archive Folder**: Reads all source Excel files and categorizes them
2. **Analyzes Coverage**: Determines what data should exist based on source files
3. **Verifies Output**: Compares pipeline output against expected data
4. **Color-Codes Results**: Creates annotated Excel with:
   - GREEN cells: Data present
   - YELLOW cells: Data missing (no source file)
   - RED cells: Expected data missing
5. **Generates Report**: Lists all missing categories and file statistics

## How to Use

1. **Update Configuration**:
   Edit `config.py` and set:
   - `ARCHIVE_FOLDER`: Path to archived source files
   - `OUTPUT_FILE`: Path to pipeline output to verify (DATA file or Master)

2. **Run Verification**:
   ```bash
   cd verify
   python verify_coverage.py
   ```

3. **Check Results**:
   - Results saved in `verify/run_YYYYMMDD_HHMMSS/` folder
   - `verification_annotated.xlsx`: Color-coded Excel file
   - `missing_categories.txt`: Text report of missing data

## Output Files

Each verification run creates a timestamped folder with:

### verification_annotated.xlsx
- Color-coded copy of output file
- Legend at top showing color meanings
- GREEN = data present, YELLOW = missing

### missing_categories.txt
- List of columns with no data
- Source file statistics
- Date coverage summary

## Example Output

```
SOURCE FILE ANALYSIS
Total source files: 225

File types:
  FX_PAIR: 127 files
  CCY_POS: 56 files

OUTPUT VERIFICATION
Columns with data: 540/540 (100.0%)
Total cells filled: 2,460

[EXCELLENT] All 540 columns have data!
```

## Configuration Options

| Setting | Description | Default |
|---------|-------------|---------|
| `ARCHIVE_FOLDER` | Path to source files | Required |
| `OUTPUT_FILE` | Output file to verify | Required |
| `CREATE_RUN_FOLDER` | Create timestamped folders | True |
| `VERBOSE` | Show detailed progress | True |
| `MAX_CONSOLE_EXAMPLES` | Examples to show | 10 |

## Troubleshooting

**Issue**: "Archive directory not found"
- Check `ARCHIVE_FOLDER` path in config.py
- Ensure path uses raw string (r'...')

**Issue**: "No files found"
- Verify archive folder contains .xlsx files
- Check folder structure

**Issue**: All files show as UNKNOWN type
- Files may have non-standard formats
- Check first few rows of source files
