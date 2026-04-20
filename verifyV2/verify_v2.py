"""
Source Data Coverage Verification Tool V2
==========================================
Uses source_column_mapping.csv as ground truth for a proper four-way
cross-reference instead of guessing from archive file headers.

Four findings it reports:
  OK       - source file processed, output column has data
  GAP      - source file processed, output column is EMPTY  (real problem)
  UNPROC   - archive file exists but was never processed by the pipeline
  ORPHAN   - output column has data but no source file in mapping

What it auto-discovers:
  - Pipeline output:    Latest run in ../output/  (CFXPP_DATA_*.xlsx)
  - Source mapping:     source_column_mapping.csv from same output run
  - Archive directory:  Latest folder in ../archive/

Usage:
  Run: python verify_v2.py   (or double-click run_verification.bat)

Settings are in config_v2.py.
"""

import sys
import re
import csv
import glob
import os
from pathlib import Path
from collections import defaultdict
from datetime import datetime

import openpyxl
from openpyxl.styles import PatternFill, Font
from openpyxl.utils import get_column_letter

import config_v2 as config

# ---------------------------------------------------------------------------
# Fills
# ---------------------------------------------------------------------------
GREEN_FILL  = PatternFill(start_color='CCFFCC', end_color='CCFFCC', fill_type='solid')  # OK
RED_FILL    = PatternFill(start_color='FFCCCC', end_color='FFCCCC', fill_type='solid')  # GAP
YELLOW_FILL = PatternFill(start_color='FFFFCC', end_color='FFFFCC', fill_type='solid')  # UNPROC
GREY_FILL   = PatternFill(start_color='E0E0E0', end_color='E0E0E0', fill_type='solid')  # no mapping
BOLD        = Font(bold=True)


# ---------------------------------------------------------------------------
# Path Discovery
# ---------------------------------------------------------------------------

def _sorted_subdirs(parent):
    if not os.path.isdir(parent):
        return []
    return sorted(e for e in os.listdir(parent) if os.path.isdir(os.path.join(parent, e)))


def find_latest_output_run(project_root):
    """Return (data_xlsx, mapping_csv) from the most-recent timestamped output run."""
    output_root = os.path.join(project_root, 'output')
    ts_pattern  = re.compile(r'^\d{8}_\d{6}$')
    ts_subdirs  = [s for s in _sorted_subdirs(output_root) if ts_pattern.match(s)]

    if not ts_subdirs:
        raise FileNotFoundError(f'No timestamped run folders found in: {output_root}')

    for folder_name in reversed(ts_subdirs):
        folder     = os.path.join(output_root, folder_name)
        data_files = glob.glob(os.path.join(folder, 'CFXPP_DATA_*.xlsx'))
        if data_files:
            mapping = os.path.join(folder, 'source_column_mapping.csv')
            return data_files[0], mapping

    raise FileNotFoundError(f'No CFXPP_DATA_*.xlsx found under: {output_root}')


def find_latest_archive_dir(project_root):
    archive_root = os.path.join(project_root, 'archive')
    subdirs = _sorted_subdirs(archive_root)
    if not subdirs:
        raise FileNotFoundError(f'No archive folders found in: {archive_root}')
    return os.path.join(archive_root, subdirs[-1])


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

def load_mapping(mapping_csv):
    """
    Returns:
        rows            - all raw mapping rows (list of dicts)
        col_map         - {Output_Column_Code: [rows]}  (one code may have multiple source rows)
        mapped_files    - set of Source_File names that were processed
    """
    rows     = []
    col_map  = defaultdict(list)

    if not os.path.exists(mapping_csv):
        raise FileNotFoundError(f'source_column_mapping.csv not found: {mapping_csv}')

    with open(mapping_csv, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            rows.append(row)
            col_map[row['Output_Column_Code']].append(row)

    mapped_files = {r['Source_File'] for r in rows}

    if config.VERBOSE:
        print(f'  Mapping loaded: {len(rows)} rows, '
              f'{len(col_map)} unique column codes, '
              f'{len(mapped_files)} source files')

    return rows, col_map, mapped_files


def load_output_data(data_xlsx):
    """
    Returns:
        codes       - list of column codes (row 1, cols 2-541, 0-indexed → codes[0] = col B)
        cells       - {(date_str, col_idx): value}  col_idx matches codes list
        dates       - sorted list of date strings in the output
    """
    if config.VERBOSE:
        print(f'  Loading output: {os.path.basename(data_xlsx)}')

    wb = openpyxl.load_workbook(data_xlsx, data_only=True)
    ws = wb['DATA']

    codes = [ws.cell(row=1, column=c).value for c in range(2, 542)]

    cells = {}
    dates = []
    for r in range(2, ws.max_row + 1):
        dv = ws.cell(row=r, column=1).value
        if dv is None:
            continue
        d = dv.strftime('%Y-%m-%d') if hasattr(dv, 'strftime') else str(dv)
        dates.append(d)
        for c in range(2, 542):
            v = ws.cell(row=r, column=c).value
            if v is not None:
                cells[(d, c - 2)] = v

    wb.close()
    return codes, cells, sorted(set(dates))


def get_archive_xlsx_files(archive_dir):
    """Return set of .xlsx filenames (basename only) in archive_dir."""
    if not os.path.isdir(archive_dir):
        return set()
    return {f for f in os.listdir(archive_dir) if f.lower().endswith('.xlsx')}


# ---------------------------------------------------------------------------
# Four-way cross-reference
# ---------------------------------------------------------------------------

def cross_reference(codes, cells, dates, col_map, mapped_files, archive_files):
    """
    Build the four-way finding sets.

    Returns a dict with keys:
        ok_codes        - set of column codes that are mapped AND have data
        gap_codes       - set of column codes that are mapped BUT have zero data
        unproc_files    - set of archive filenames never processed
        orphan_codes    - set of column codes with data but NOT in mapping
        col_status      - {col_code: 'OK' | 'GAP' | 'ORPHAN' | 'EMPTY'}
        per_date        - {date: {'filled': n, 'empty': n}}
        per_section     - {section: {'ok': n, 'gap': n}}
        gap_detail      - [{code, section, description, source_files}]
        unproc_detail   - [{filename, is_duplicate_suffix}]
    """
    mapped_codes = set(col_map.keys())

    # Which codes have actual data in the output?
    codes_with_data = set()
    for (date, idx), val in cells.items():
        if idx < len(codes) and codes[idx]:
            codes_with_data.add(codes[idx])

    ok_codes     = mapped_codes & codes_with_data
    gap_codes    = mapped_codes - codes_with_data
    orphan_codes = codes_with_data - mapped_codes

    # Unprocessed archive files
    unproc_files = archive_files - mapped_files

    # Per-date coverage
    per_date = defaultdict(lambda: {'filled': 0, 'empty': 0})
    for d in dates:
        for idx, code in enumerate(codes):
            if code is None:
                continue
            if (d, idx) in cells:
                per_date[d]['filled'] += 1
            else:
                per_date[d]['empty'] += 1

    # Per-section coverage
    per_section = defaultdict(lambda: {'ok': 0, 'gap': 0})
    for code in ok_codes:
        rows = col_map.get(code, [{}])
        section = rows[0].get('Section', 'UNKNOWN')
        per_section[section]['ok'] += 1
    for code in gap_codes:
        rows = col_map.get(code, [{}])
        section = rows[0].get('Section', 'UNKNOWN')
        per_section[section]['gap'] += 1

    # Gap detail
    gap_detail = []
    for code in sorted(gap_codes):
        rows = col_map[code]
        gap_detail.append({
            'code':         code,
            'section':      rows[0].get('Section', ''),
            'description':  rows[0].get('Output_Column_Description', ''),
            'source_files': [r['Source_File'] for r in rows],
        })

    # Unprocessed detail — flag likely duplicates (_1.xlsx suffix pattern)
    dup_pattern = re.compile(r'_\d+\.xlsx$', re.IGNORECASE)
    unproc_detail = sorted([
        {
            'filename':             fn,
            'likely_duplicate':     bool(dup_pattern.search(fn)),
        }
        for fn in unproc_files
    ], key=lambda x: x['filename'])

    # Column status map for annotated Excel
    col_status = {}
    for code in codes:
        if code is None:
            continue
        if code in ok_codes:
            col_status[code] = 'OK'
        elif code in gap_codes:
            col_status[code] = 'GAP'
        elif code in orphan_codes:
            col_status[code] = 'ORPHAN'
        else:
            col_status[code] = 'EMPTY'   # not mapped, no data

    return {
        'ok_codes':      ok_codes,
        'gap_codes':     gap_codes,
        'orphan_codes':  orphan_codes,
        'unproc_files':  unproc_files,
        'col_status':    col_status,
        'per_date':      per_date,
        'per_section':   per_section,
        'gap_detail':    gap_detail,
        'unproc_detail': unproc_detail,
    }


# ---------------------------------------------------------------------------
# Annotated Excel
# ---------------------------------------------------------------------------

def create_annotated_excel(data_xlsx, output_path, codes, col_status):
    """Copy DATA file and highlight each column header by status."""
    if config.VERBOSE:
        print(f'  Creating annotated Excel: {os.path.basename(output_path)}')

    import shutil
    shutil.copy2(data_xlsx, output_path)

    wb = openpyxl.load_workbook(output_path)
    ws = wb['DATA']

    fill_map = {
        'OK':     GREEN_FILL,
        'GAP':    RED_FILL,
        'ORPHAN': YELLOW_FILL,
        'EMPTY':  GREY_FILL,
    }

    # Insert 2 legend rows at top
    ws.insert_rows(1, 2)

    ws.cell(1, 1, 'LEGEND:').font = BOLD
    ws.cell(1, 2, 'GREEN  = Mapped + has data (OK)').fill     = GREEN_FILL
    ws.cell(1, 3, 'RED    = Mapped + NO data (GAP)').fill     = RED_FILL
    ws.cell(1, 4, 'YELLOW = Data present, no source (ORPHAN)').fill = YELLOW_FILL
    ws.cell(1, 5, 'GREY   = Not mapped, no data').fill        = GREY_FILL
    ws.cell(1, 6, f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

    # Colour column headers (now on row 3 after insert)
    for c in range(2, 542):
        code = ws.cell(3, c).value
        if code and code in col_status:
            ws.cell(3, c).fill = fill_map.get(col_status[code], GREY_FILL)

    wb.save(output_path)
    wb.close()


# ---------------------------------------------------------------------------
# Text report
# ---------------------------------------------------------------------------

def write_text_report(report_path, findings, data_xlsx, mapping_csv, archive_dir, dates):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    ok    = len(findings['ok_codes'])
    gap   = len(findings['gap_codes'])
    orph  = len(findings['orphan_codes'])
    total_mapped = ok + gap

    unproc_all  = findings['unproc_detail']
    unproc_dup  = [u for u in unproc_all if u['likely_duplicate']]
    unproc_real = [u for u in unproc_all if not u['likely_duplicate']]

    with open(report_path, 'w', encoding='utf-8') as f:

        def hr(char='='):
            f.write(char * 80 + '\n')

        hr()
        f.write('CFXPP COVERAGE VERIFICATION REPORT V2\n')
        hr()
        f.write(f'Generated:      {ts}\n')
        f.write(f'Output file:    {os.path.basename(data_xlsx)}\n')
        f.write(f'Mapping CSV:    {os.path.basename(mapping_csv)}\n')
        f.write(f'Archive folder: {os.path.basename(archive_dir)}\n')
        f.write(f'Date range:     {dates[0]} → {dates[-1]}  ({len(dates)} dates)\n')
        f.write('\n')

        # ── Summary ────────────────────────────────────────────────────────
        hr()
        f.write('SUMMARY\n')
        hr()
        f.write(f'  Mapped columns with data    (OK):     {ok:>4}\n')
        f.write(f'  Mapped columns with NO data (GAP):    {gap:>4}  ← real problems\n')
        f.write(f'  Columns with data, no source (ORPHAN):{orph:>4}\n')
        f.write(f'  Archive files not processed:          {len(unproc_all):>4}\n')
        f.write(f'    of which likely duplicates:         {len(unproc_dup):>4}\n')
        f.write(f'    of which genuinely unprocessed:     {len(unproc_real):>4}\n')
        f.write('\n')

        match_pct = (ok / total_mapped * 100) if total_mapped else 0
        f.write(f'  Coverage: {ok}/{total_mapped} mapped columns have data ({match_pct:.1f}%)\n')
        f.write('\n')

        # ── Per-section ────────────────────────────────────────────────────
        hr()
        f.write('COVERAGE BY SECTION\n')
        hr()
        for section, counts in sorted(findings['per_section'].items()):
            total_s = counts['ok'] + counts['gap']
            pct     = (counts['ok'] / total_s * 100) if total_s else 0
            flag    = '  ' if counts['gap'] == 0 else '! '
            f.write(f'  {flag}{section:<25}  {counts["ok"]:>3} ok  {counts["gap"]:>3} gap  ({pct:.0f}%)\n')
        f.write('\n')

        # ── Per-date ───────────────────────────────────────────────────────
        hr()
        f.write('COVERAGE BY DATE\n')
        hr()
        f.write(f'  {"Date":<12}  {"Filled":>7}  {"Empty":>7}  {"Coverage":>9}\n')
        f.write(f'  {"-"*12}  {"-"*7}  {"-"*7}  {"-"*9}\n')
        for date in sorted(findings['per_date']):
            d    = findings['per_date'][date]
            tot  = d['filled'] + d['empty']
            pct  = (d['filled'] / tot * 100) if tot else 0
            flag = '  ' if pct >= 99 else '! '
            f.write(f'  {flag}{date:<12}  {d["filled"]:>7,}  {d["empty"]:>7,}  {pct:>8.1f}%\n')
        f.write('\n')

        # ── GAP detail ─────────────────────────────────────────────────────
        hr()
        f.write(f'GAPS — MAPPED COLUMNS WITH ZERO DATA ({gap})\n')
        hr()
        if not findings['gap_detail']:
            f.write('  None — all mapped columns have data.\n')
        else:
            for i, g in enumerate(findings['gap_detail'], 1):
                f.write(f'  {i:>3}. {g["code"]}\n')
                f.write(f'       Section:     {g["section"]}\n')
                f.write(f'       Description: {g["description"]}\n')
                f.write(f'       Source file: {", ".join(g["source_files"])}\n')
        f.write('\n')

        # ── Genuinely unprocessed ──────────────────────────────────────────
        hr()
        f.write(f'GENUINELY UNPROCESSED ARCHIVE FILES ({len(unproc_real)})\n')
        hr('-')
        f.write('  These are in the archive but not in the mapping and do not look\n')
        f.write('  like duplicate exports (no numeric suffix before .xlsx).\n\n')
        if not unproc_real:
            f.write('  None.\n')
        else:
            for u in unproc_real:
                f.write(f'  {u["filename"]}\n')
        f.write('\n')

        # ── Duplicate-suffix unprocessed ───────────────────────────────────
        hr()
        f.write(f'LIKELY DUPLICATE EXPORTS (skipped by pipeline) ({len(unproc_dup)})\n')
        hr('-')
        f.write('  Files with a numeric suffix (_1, _2 …) — pipeline intentionally\n')
        f.write('  skips these as duplicate exports of the same source data.\n\n')
        if config.VERBOSE_UNPROC_DUPLICATES:
            for u in unproc_dup:
                f.write(f'  {u["filename"]}\n')
        else:
            f.write(f'  (set VERBOSE_UNPROC_DUPLICATES = True in config_v2.py to list them)\n')
        f.write('\n')

        # ── Orphans ────────────────────────────────────────────────────────
        if findings['orphan_codes']:
            hr()
            f.write(f'ORPHAN COLUMNS — data present but no source mapping ({orph})\n')
            hr('-')
            for code in sorted(findings['orphan_codes']):
                f.write(f'  {code}\n')
            f.write('\n')

        hr()
        f.write('END OF REPORT\n')
        hr()


# ---------------------------------------------------------------------------
# Console summary
# ---------------------------------------------------------------------------

def print_summary(findings, run_folder):
    ok   = len(findings['ok_codes'])
    gap  = len(findings['gap_codes'])
    orph = len(findings['orphan_codes'])
    unproc_real = [u for u in findings['unproc_detail'] if not u['likely_duplicate']]
    total_mapped = ok + gap
    pct = (ok / total_mapped * 100) if total_mapped else 0

    print(f"\n{'='*80}")
    print('VERIFICATION COMPLETE')
    print(f"{'='*80}")
    print(f"  Mapped + has data  (OK):            {ok:>4}  ({pct:.1f}%)")
    print(f"  Mapped + NO data   (GAP):            {gap:>4}  << investigate these")
    print(f"  Data, no source    (ORPHAN):         {orph:>4}")
    print(f"  Genuinely unprocessed archive files: {len(unproc_real):>4}")
    if gap == 0:
        print("\n  [EXCELLENT] All mapped columns have data!")
    elif gap <= 10:
        print(f"\n  [GOOD] Only {gap} gap(s) — see report for details")
    else:
        print(f"\n  [ACTION NEEDED] {gap} columns have no data")

    if findings['gap_detail']:
        print('\n  Top gaps:')
        for g in findings['gap_detail'][:5]:
            print(f"    {g['code']}")
        if len(findings['gap_detail']) > 5:
            print(f"    ... and {len(findings['gap_detail']) - 5} more (see report)")

    print(f'\n  Output folder: {run_folder}')
    print(f"{'='*80}\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    print('=' * 80)
    print('CFXPP COVERAGE VERIFICATION TOOL  V2')
    print('=' * 80)

    try:
        verify_v2_dir = os.path.dirname(os.path.abspath(__file__))
        project_root  = os.path.dirname(verify_v2_dir)

        print('\nDiscovering paths...')
        data_xlsx, mapping_csv = find_latest_output_run(project_root)
        archive_dir            = find_latest_archive_dir(project_root)

        print(f'  Output file:    {data_xlsx}')
        print(f'  Mapping CSV:    {mapping_csv}')
        print(f'  Archive folder: {archive_dir}')

        # Load everything
        print('\nLoading data...')
        mapping_rows, col_map, mapped_files = load_mapping(mapping_csv)
        codes, cells, dates                 = load_output_data(data_xlsx)
        archive_files                       = get_archive_xlsx_files(archive_dir)

        if config.VERBOSE:
            print(f'  Output columns: {len([c for c in codes if c])} with codes')
            print(f'  Output cells:   {len(cells):,} non-empty')
            print(f'  Archive files:  {len(archive_files)} xlsx files')

        # Cross-reference
        print('\nCross-referencing...')
        findings = cross_reference(codes, cells, dates, col_map, mapped_files, archive_files)

        # Build run folder
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        run_folder = Path(verify_v2_dir) / f'run_{timestamp}'
        run_folder.mkdir(parents=True, exist_ok=True)

        # Write outputs
        print('\nWriting reports...')
        excel_path  = run_folder / 'verification_annotated.xlsx'
        report_path = run_folder / 'verification_report.txt'

        create_annotated_excel(data_xlsx, str(excel_path), codes, findings['col_status'])
        write_text_report(str(report_path), findings, data_xlsx, mapping_csv, archive_dir, dates)

        if config.VERBOSE:
            print(f'  {excel_path.name}')
            print(f'  {report_path.name}')

        print_summary(findings, run_folder)

    except FileNotFoundError as e:
        print(f'\nERROR: {e}')
        sys.exit(1)

    except KeyboardInterrupt:
        print('\n\nInterrupted by user')
        sys.exit(130)

    except Exception as e:
        print(f'\nERROR: {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
