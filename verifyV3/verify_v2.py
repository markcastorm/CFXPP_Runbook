"""
Source Data Coverage Verification Tool V2
==========================================
Dynamic path discovery - no manual path entry needed.

What it auto-discovers:
  - Pipeline output:    Latest run in ../output/  (CFXPP_DATA_*.xlsx)
  - Archive directory:  Latest folder in ../archive/

Usage:
  1. Run: python verify_v2.py
  2. Find results in the new run_YYYYMMDD_HHMMSS/ folder

Settings (verbosity, display) are in config_v2.py.
"""

import sys
import re
import glob
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
import openpyxl
from openpyxl.styles import PatternFill, Font
from openpyxl.utils import get_column_letter
from collections import defaultdict
from datetime import datetime

import config_v2 as config

# Color schemes
RED_FILL    = PatternFill(start_color='FFCCCC', end_color='FFCCCC', fill_type='solid')
YELLOW_FILL = PatternFill(start_color='FFFFCC', end_color='FFFFCC', fill_type='solid')
GREEN_FILL  = PatternFill(start_color='CCFFCC', end_color='CCFFCC', fill_type='solid')
ORANGE_FILL = PatternFill(start_color='FFE5CC', end_color='FFE5CC', fill_type='solid')


# ---------------------------------------------------------------------------
# Path Discovery
# ---------------------------------------------------------------------------

def _sorted_subdirs(parent):
    """Return subdirectories of *parent* sorted ascending (oldest first)."""
    if not os.path.isdir(parent):
        return []
    return sorted(
        e for e in os.listdir(parent)
        if os.path.isdir(os.path.join(parent, e))
    )


def find_latest_output_run(project_root):
    """
    Locate the most-recent YYYYMMDD_HHMMSS folder under <project_root>/output/
    and return the CFXPP_DATA_*.xlsx path inside it.
    """
    output_root = os.path.join(project_root, 'output')
    subdirs = _sorted_subdirs(output_root)

    ts_pattern = re.compile(r'^\d{8}_\d{6}$')
    ts_subdirs = [s for s in subdirs if ts_pattern.match(s)]

    if not ts_subdirs:
        raise FileNotFoundError(
            f'No timestamped run folders (YYYYMMDD_HHMMSS) found in: {output_root}'
        )

    for folder_name in reversed(ts_subdirs):
        folder = os.path.join(output_root, folder_name)
        data_files = glob.glob(os.path.join(folder, 'CFXPP_DATA_*.xlsx'))
        if data_files:
            return data_files[0]

    raise FileNotFoundError(
        f'No CFXPP_DATA_*.xlsx found in any output run folder under: {output_root}'
    )


def find_latest_archive_dir(project_root):
    """
    Return the path to the most-recent (non-zip) folder under
    <project_root>/archive/.  Raises if none found.
    """
    archive_root = os.path.join(project_root, 'archive')
    subdirs = _sorted_subdirs(archive_root)
    if not subdirs:
        raise FileNotFoundError(f'No archive folders found in: {archive_root}')
    return os.path.join(archive_root, subdirs[-1])


# ---------------------------------------------------------------------------
# Source file analysis  (unchanged from V1)
# ---------------------------------------------------------------------------

def parse_source_file_metadata(file_path):
    """Extract metadata from a source file by reading its content."""
    try:
        wb = openpyxl.load_workbook(file_path, data_only=True)
        ws = wb.active

        header_data = {}
        for row_idx in range(1, min(15, ws.max_row + 1)):
            for col_idx in range(1, min(10, ws.max_column + 1)):
                cell = ws.cell(row_idx, col_idx)
                if cell.value:
                    header_data[f"R{row_idx}C{col_idx}"] = str(cell.value)

        wb.close()

        file_info = {
            'file_path': str(file_path),
            'file_name': file_path.name,
            'file_type': 'UNKNOWN',
            'currency_pair': None,
            'currency': None,
            'client_type': None,
            'date': None,
            'section': None,
        }

        header_text = ' '.join(header_data.values()).upper()

        fx_pairs = [
            'AUDUSD', 'EURUSD', 'GBPUSD', 'USDJPY', 'USDCAD', 'NZDUSD', 'USDCHF',
            'USDMXN', 'USDBRL', 'USDCOP', 'USDZAR', 'USDTRY', 'USDSGD', 'USDINR',
            'USDPLN', 'USDCZK', 'USDHUF', 'EURGBP', 'EURJPY', 'EURCHF', 'GBPJPY',
        ]
        for pair in fx_pairs:
            if pair in header_text:
                file_info['file_type'] = 'FX_PAIR'
                file_info['currency_pair'] = pair
                break

        if 'CURRENCY POSITIONING' in header_text or 'CUMULATIVE POSITIONS' in header_text:
            file_info['file_type'] = 'CCY_POS'
            if 'G10' in header_text:
                file_info['section'] = 'G10'
            elif 'EM' in header_text or 'EMERGING' in header_text:
                file_info['section'] = 'EM'

            currencies = [
                'USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD', 'NZD', 'SEK', 'NOK',
                'MXN', 'BRL', 'COP', 'ZAR', 'TRY', 'SGD', 'INR', 'PLN', 'CZK', 'HUF',
            ]
            for ccy in currencies:
                if ccy in header_text:
                    file_info['currency'] = ccy
                    break

        client_types = [
            'BANKS', 'BROKER', 'CORPORATE', 'HEDGE FUND', 'HEDGEFUND',
            'REAL MONEY', 'REALMONEY', 'UNCLASSIFIED',
        ]
        found_types = []
        for ct in client_types:
            if ct in header_text:
                found_types.append(ct.replace(' ', '_'))
        if found_types:
            file_info['client_type'] = '_'.join(sorted(set(found_types)))

        date_match = re.search(r'(\d{2})-(\d{2})-(\d{4})', file_path.name)
        if date_match:
            month, day, year = date_match.groups()
            file_info['date'] = f"{year}-{month}-{day}"

        return file_info

    except Exception as e:
        return {
            'file_path': str(file_path),
            'file_name': file_path.name,
            'file_type': 'ERROR',
            'error': str(e),
        }


def scan_archive_folder(archive_path):
    """Scan archive folder and categorize source files."""
    if config.VERBOSE:
        print(f"\nScanning archive folder: {archive_path}")

    archive_dir = Path(archive_path)

    if not archive_dir.exists():
        print(f"ERROR: Archive directory not found: {archive_path}")
        return None

    files = list(archive_dir.glob("**/*.xlsx"))
    if config.VERBOSE:
        print(f"Found {len(files)} Excel files")
        print("\nAnalyzing file contents (this may take a moment)...")

    file_catalog = []
    for idx, fpath in enumerate(files):
        if config.VERBOSE and (idx + 1) % 25 == 0:
            print(f"  Analyzed {idx + 1}/{len(files)} files...")
        file_catalog.append(parse_source_file_metadata(fpath))

    if config.VERBOSE:
        print(f"  Analyzed {len(files)}/{len(files)} files - Complete!")

    return file_catalog


def analyze_expected_coverage(file_catalog):
    """Determine what output columns SHOULD exist based on source files."""
    fx_files  = defaultdict(list)
    ccy_files = defaultdict(list)

    for file_info in file_catalog:
        file_type = file_info['file_type']
        date = file_info.get('date')

        if file_type == 'FX_PAIR':
            pair   = file_info.get('currency_pair')
            client = file_info.get('client_type', 'UNKNOWN')
            if pair and date:
                fx_files[(pair, client)].append(date)

        elif file_type == 'CCY_POS':
            currency = file_info.get('currency')
            client   = file_info.get('client_type', 'UNKNOWN')
            section  = file_info.get('section', 'UNKNOWN')
            if currency and date:
                ccy_files[(currency, client, section)].append(date)

    return {'fx_files': fx_files, 'ccy_files': ccy_files, 'file_catalog': file_catalog}


# ---------------------------------------------------------------------------
# Verification & reporting
# ---------------------------------------------------------------------------

def verify_output_against_sources(output_file, source_analysis, report_dir):
    """Verify output file against source files and create annotated report."""
    print(f"\n{'='*100}")
    print("VERIFICATION REPORT")
    print(f"{'='*100}")

    output_df = pd.read_excel(output_file)
    print(f"\nOutput file: {output_file}")
    print(f"Shape: {output_df.shape}")
    print(f"Dates: {output_df.iloc[:, 0].tolist()}")

    fx_files      = source_analysis['fx_files']
    ccy_files     = source_analysis['ccy_files']
    file_catalog  = source_analysis['file_catalog']

    print(f"\n{'='*100}")
    print("SOURCE FILE ANALYSIS")
    print(f"{'='*100}")
    print(f"Total source files: {len(file_catalog)}")

    file_types = defaultdict(int)
    for f in file_catalog:
        file_types[f['file_type']] += 1

    print("\nFile types found:")
    for ftype, count in sorted(file_types.items()):
        print(f"  {ftype}: {count} files")

    dates_found = {f['date'] for f in file_catalog if f.get('date')}
    print(f"\nDates found in source files: {sorted(dates_found)}")

    print(f"\n{'='*100}")
    print("FX PAIR SOURCE FILES")
    print(f"{'='*100}")
    print(f"Unique (Pair, Client) combinations: {len(fx_files)}")
    if fx_files:
        sorted_fx = sorted(fx_files.items(), key=lambda x: (str(x[0][0] or ''), str(x[0][1] or '')))
        for (pair, client), dates in sorted_fx[:config.MAX_CONSOLE_EXAMPLES]:
            print(f"  {(pair or 'N/A'):10} + {(client or 'N/A'):40} -> {len(dates)} dates")
        if len(fx_files) > config.MAX_CONSOLE_EXAMPLES:
            print(f"  ... and {len(fx_files) - config.MAX_CONSOLE_EXAMPLES} more combinations")
    else:
        print("  (No FX Pair files found)")

    print(f"\n{'='*100}")
    print("CURRENCY POSITIONING SOURCE FILES")
    print(f"{'='*100}")
    print(f"Unique (Currency, Client, Section) combinations: {len(ccy_files)}")
    sorted_ccy = sorted(ccy_files.items(), key=lambda x: (str(x[0][2] or ''), str(x[0][0] or ''), str(x[0][1] or '')))
    for (ccy, client, section), dates in sorted_ccy[:config.MAX_CONSOLE_EXAMPLES]:
        print(f"  {(section or 'N/A'):5} {(ccy or 'N/A'):5} + {(client or 'N/A'):40} -> {len(dates)} dates")
    if len(ccy_files) > config.MAX_CONSOLE_EXAMPLES:
        print(f"  ... and {len(ccy_files) - config.MAX_CONSOLE_EXAMPLES} more combinations")

    print(f"\n{'='*100}")
    print("OUTPUT VERIFICATION")
    print(f"{'='*100}")

    data_cols     = output_df.iloc[:, 1:]
    total_cols    = len(data_cols.columns)
    cols_with_data = (data_cols.count() > 0).sum()
    filled_cells  = data_cols.count().sum()

    print(f"Total columns: {total_cols}")
    print(f"Columns with data: {cols_with_data}/{total_cols} ({100*cols_with_data/total_cols:.1f}%)")
    print(f"Total cells filled: {filled_cells:,}")

    # Build output directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if config.CREATE_RUN_FOLDER:
        run_folder  = Path(report_dir) / f"run_{timestamp}"
        run_folder.mkdir(parents=True, exist_ok=True)
        output_path = run_folder / "verification_annotated.xlsx"
        report_path = run_folder / "missing_categories.txt"
    else:
        output_path = Path(report_dir) / f"verification_report_{timestamp}.xlsx"
        report_path = Path(report_dir) / f"missing_categories_{timestamp}.txt"

    print("\nCreating annotated report...")

    wb = openpyxl.load_workbook(output_file)
    ws = wb.active

    ws.insert_rows(1, 4)
    ws['A1'] = "VERIFICATION LEGEND:"
    ws['A2'] = "GREEN: Data present and verified"
    ws['A2'].fill = GREEN_FILL
    ws['A3'] = "YELLOW: Data missing (no source file found)"
    ws['A3'].fill = YELLOW_FILL
    ws['A4'] = "RED: Data expected but missing in output"
    ws['A4'].fill = RED_FILL

    if config.VERBOSE:
        print("  Analyzing and coloring cells...")

    missing_categories = set()

    for col_idx in range(2, ws.max_column + 1):
        col_has_data = False
        for row_idx in range(6, ws.max_row + 1):
            cell = ws.cell(row_idx, col_idx)
            if cell.value is not None and cell.value != '':
                cell.fill = GREEN_FILL
                col_has_data = True
            else:
                cell.fill = YELLOW_FILL

        if not col_has_data:
            col_letter  = get_column_letter(col_idx)
            header_cell = ws.cell(5, col_idx)
            col_name    = str(header_cell.value) if header_cell.value else f"Column_{col_letter}"
            missing_categories.add(col_name[:100])

    if config.VERBOSE:
        print("  Saving annotated file...")
    wb.save(output_path)
    print(f"  Saved: {output_path}")

    # Missing categories report
    with open(report_path, 'w') as f:
        f.write("=" * 100 + "\n")
        f.write("MISSING CATEGORIES REPORT\n")
        f.write("=" * 100 + "\n\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Output file: {output_file}\n")
        f.write(f"Archive folder: {source_analysis.get('archive_path', 'N/A')}\n\n")
        f.write(f"Total columns: {total_cols}\n")
        f.write(f"Columns with data: {cols_with_data}\n")
        f.write(f"Columns missing data: {len(missing_categories)}\n\n")
        f.write("=" * 100 + "\n")
        f.write("COLUMNS WITH NO DATA (Missing Source Files)\n")
        f.write("=" * 100 + "\n\n")
        for idx, cat in enumerate(sorted(missing_categories), 1):
            f.write(f"{idx:4}. {cat}\n")
        if not missing_categories:
            f.write("  (None - all columns have data!)\n")
        f.write("\n" + "=" * 100 + "\n")
        f.write("SOURCE FILE SUMMARY\n")
        f.write("=" * 100 + "\n\n")
        f.write(f"Total source files: {len(file_catalog)}\n\n")
        f.write("File types:\n")
        for ftype, count in sorted(file_types.items()):
            f.write(f"  {ftype}: {count} files\n")
        f.write(f"\nDates found: {', '.join(sorted(dates_found))}\n")
        f.write(f"\nFX Pair files: {len(fx_files)} unique combinations\n")
        f.write(f"Currency Positioning files: {len(ccy_files)} unique combinations\n")

    print(f"  Saved: {report_path}")

    # Console summary
    print(f"\n{'='*100}")
    print("VERIFICATION SUMMARY")
    print(f"{'='*100}")
    if config.CREATE_RUN_FOLDER:
        print(f"Output folder: {run_folder}")
    print(f"  - Columns with data: {cols_with_data}/{total_cols} ({100*cols_with_data/total_cols:.1f}%)")
    print(f"  - Columns missing data: {len(missing_categories)}")

    if len(missing_categories) == 0:
        print(f"\n[EXCELLENT] All {total_cols} columns have data!")
    elif cols_with_data >= 400:
        print(f"\n[GOOD] {cols_with_data}/{total_cols} columns covered")
    else:
        print(f"\n[FAIR] {cols_with_data}/{total_cols} columns covered - {len(missing_categories)} categories missing")

    print(f"\n{'='*100}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    print("=" * 100)
    print("SOURCE DATA COVERAGE VERIFICATION TOOL  V2  (dynamic paths)")
    print("=" * 100)

    try:
        verify_v2_dir = os.path.dirname(os.path.abspath(__file__))
        project_root  = os.path.dirname(verify_v2_dir)

        print("\nDiscovering paths...")

        output_file  = find_latest_output_run(project_root)
        archive_dir  = find_latest_archive_dir(project_root)
        report_dir   = verify_v2_dir

        print(f"\nConfiguration:")
        print(f"  Output file:    {output_file}")
        print(f"  Archive folder: {archive_dir}")
        print(f"  Report dir:     {report_dir}")

        file_catalog = scan_archive_folder(archive_dir)

        if not file_catalog:
            print("\nERROR: No files found or archive folder error")
            return

        source_analysis = analyze_expected_coverage(file_catalog)
        source_analysis['archive_path'] = archive_dir

        verify_output_against_sources(output_file, source_analysis, report_dir)

        print(f"\n{'='*100}")
        print("VERIFICATION COMPLETE!")
        print(f"{'='*100}\n")

    except FileNotFoundError as e:
        print(f"\nERROR: {e}")
        sys.exit(1)

    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(130)

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
