"""
CFXPP Orchestrator
===================
Main entry point for the CFXPP runbook pipeline.

Pipeline:
1. Scan input folder recursively for Excel files
2. Parse all files in parallel (multiprocessing)
3. Identify batch from date ranges
4. Load existing master (for incremental updates)
5. Map parsed results to output columns
6. Build/merge output DataFrame
7. Generate DATA + META + ZIP output files
8. Save coverage report CSV
9. Archive processed files
10. Print summary

Usage:
    python orchestrator.py
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd

import config
from logger_setup import setup_logging
from data_loader import CFXPPDataLoader
from parser import parse_single_file
from column_mapper import CFXPPColumnMapper
from file_generator import CFXPPFileGenerator
from tracker import CFXPPTracker
from archiver import CFXPPArchiver

logger = logging.getLogger(__name__)


def print_banner():
    """Print welcome banner."""
    banner = """
====================================================================
                       CFXPP RUNBOOK
         Barclays FX Pair Positioning Data Processing
              Multiprocessing Pipeline Engine
====================================================================
    """
    print(banner)


def print_configuration():
    """Print current configuration."""
    print('Configuration:')
    print(f'  Input Directory:   {config.INPUT_DIR}')
    print(f'  Output Directory:  {config.OUTPUT_DIR}')
    print(f'  Master Directory:  {config.MASTER_DIR}')
    print(f'  Archive Directory: {config.ARCHIVE_DIR}')
    print(f'  FX Pairs:          {len(config.FX_PAIR_ORDER)}')
    print(f'  Client Types:      {len(config.CLIENT_TYPE_ORDER)}')
    print(f'  Max Workers:       {config.MAX_WORKERS}')
    print()


def identify_batch(parsed_results):
    """
    Determine batch ID from parsed results.

    Strategy:
    - Collect all start_date/end_date values
    - FX_PAIR files define the batch range (have both start and end)
    - CCY_POS files have individual dates that fall within the range
    - Batch ID = most common start_date + most common end_date

    Returns:
        str: Batch ID like '20260320_20260325'.
    """
    start_dates = []
    end_dates = []

    for result in parsed_results:
        sd = result.get('start_date')
        ed = result.get('end_date')
        if sd:
            start_dates.append(sd)
        if ed:
            end_dates.append(ed)

    if not start_dates:
        return config.get_timestamp()

    # Use most common dates
    start_counter = Counter(start_dates)
    end_counter = Counter(end_dates)

    most_common_start = start_counter.most_common(1)[0][0]
    most_common_end = end_counter.most_common(1)[0][0] if end_dates else most_common_start

    batch_id = config.get_batch_id(most_common_start, most_common_end)
    return batch_id


def get_all_dates_in_range(start_date, end_date):
    """
    Generate all dates from start_date to end_date inclusive.

    Args:
        start_date: 'YYYY-MM-DD' string.
        end_date: 'YYYY-MM-DD' string.

    Returns:
        list of 'YYYY-MM-DD' strings.
    """
    try:
        sd = datetime.strptime(start_date, '%Y-%m-%d')
        ed = datetime.strptime(end_date, '%Y-%m-%d')
    except (ValueError, TypeError):
        return [start_date]

    dates = []
    current = sd
    while current <= ed:
        dates.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)

    return dates


def build_output_dataframe(existing_df, all_updates, mapper, batch_dates):
    """
    Build or update the output DataFrame.

    Args:
        existing_df: DataFrame from master (may be empty).
        all_updates: {column_code: {date_str: value}}.
        mapper: CFXPPColumnMapper instance.
        batch_dates: List of all dates for the batch.

    Returns:
        pd.DataFrame with 'date' column + all 540 data columns.
    """
    codes = [code for code, _ in mapper.column_order]
    all_columns = ['date'] + codes

    # Start with existing data or empty
    if not existing_df.empty:
        df = existing_df.copy()
        # Ensure all columns exist
        for col in all_columns:
            if col not in df.columns:
                df[col] = None
    else:
        df = pd.DataFrame(columns=all_columns)

    # Ensure all batch dates have rows
    existing_dates = set(df['date'].tolist()) if 'date' in df.columns else set()
    for date in batch_dates:
        if date not in existing_dates:
            new_row = pd.DataFrame([{'date': date}], columns=all_columns)
            df = pd.concat([df, new_row], ignore_index=True)

    # Apply updates
    df = df.set_index('date')
    for code, date_vals in all_updates.items():
        if code in df.columns:
            for date, value in date_vals.items():
                if date in df.index:
                    df.at[date, code] = value

    df = df.reset_index()
    df = df.sort_values('date').reset_index(drop=True)

    # Reorder columns
    final_cols = [c for c in all_columns if c in df.columns]
    df = df[final_cols]

    return df


def main():
    """
    Main orchestration function.

    Returns:
        int: Exit code (0 for success, 1 for failure).
    """
    timestamp = config.get_timestamp()
    log_file = setup_logging(timestamp)

    print_banner()
    print_configuration()

    try:
        # =================================================================
        # STEP 1: Initialize
        # =================================================================
        mapper = CFXPPColumnMapper()
        tracker = CFXPPTracker(mapper.column_order, mapper.code_to_index)
        archiver = CFXPPArchiver()
        loader = CFXPPDataLoader()

        # =================================================================
        # STEP 2: Discover Input Files
        # =================================================================
        print('STEP 1: Scanning input folder...')
        logger.info('=' * 60)
        logger.info('STEP 1: Discovering input files')
        logger.info('=' * 60)

        files = loader.find_input_files()

        if not files:
            logger.info('No input files found')
            print(f'No input files found in: {config.INPUT_DIR}')
            print(f'Place input files (and subfolders) in the Input/ directory.')
            return 0

        print(f'  Found {len(files)} input file(s)')

        # =================================================================
        # STEP 3: Parse All Files with Multiprocessing
        # =================================================================
        print(f'\nSTEP 2: Parsing files ({config.MAX_WORKERS} workers)...')
        logger.info('=' * 60)
        logger.info(f'STEP 2: Parsing {len(files)} files with {config.MAX_WORKERS} workers')
        logger.info('=' * 60)

        parsed_results = []
        errors = []
        completed = 0

        with ProcessPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
            future_to_path = {
                executor.submit(parse_single_file, fp): fp
                for fp in files
            }

            for future in as_completed(future_to_path):
                path = future_to_path[future]
                completed += 1

                try:
                    result = future.result()
                    if result:
                        parsed_results.append(result)
                    else:
                        errors.append((path, 'Parse returned None'))
                        tracker.register_error(path, 'Parse returned None')
                except Exception as e:
                    errors.append((path, str(e)))
                    tracker.register_error(path, str(e))
                    logger.error(f'Failed: {os.path.basename(path)}: {e}')

                # Progress update every 25 files
                if completed % 25 == 0 or completed == len(files):
                    print(f'  Parsed {completed}/{len(files)} files...')

        print(f'  Successfully parsed: {len(parsed_results)}')
        if errors:
            print(f'  Errors: {len(errors)}')

        if not parsed_results:
            print('\nNo files could be parsed. Check logs for details.')
            return 1

        # Count by type
        fx_count = sum(1 for r in parsed_results if r['file_type'] == config.FILE_TYPE_FX_PAIR)
        ccy_count = sum(1 for r in parsed_results if r['file_type'] == config.FILE_TYPE_CCY_POS)
        print(f'  FX Pair files: {fx_count}')
        print(f'  Currency Positioning files: {ccy_count}')

        # =================================================================
        # STEP 4: Identify Batch
        # =================================================================
        print('\nSTEP 3: Identifying batch...')
        logger.info('=' * 60)
        logger.info('STEP 3: Identifying batch')
        logger.info('=' * 60)

        batch_id = identify_batch(parsed_results)
        print(f'  Batch ID: {batch_id}')

        # Determine date range
        all_start = set()
        all_end = set()
        all_single = set()
        for r in parsed_results:
            if r.get('start_date'):
                all_start.add(r['start_date'])
            if r.get('end_date'):
                all_end.add(r['end_date'])
            if r['file_type'] == config.FILE_TYPE_CCY_POS:
                all_single.add(r.get('start_date'))

        # Build full date range
        range_start = min(all_start) if all_start else None
        range_end = max(all_end) if all_end else (max(all_single) if all_single else range_start)

        if range_start and range_end:
            batch_dates = get_all_dates_in_range(range_start, range_end)
        else:
            batch_dates = sorted(all_start | all_single)

        print(f'  Date range: {range_start} to {range_end} ({len(batch_dates)} dates)')
        logger.info(f'Batch {batch_id}: {range_start} to {range_end}, {len(batch_dates)} dates')

        # =================================================================
        # STEP 5: Load Existing Master (incremental)
        # =================================================================
        print('\nSTEP 4: Checking for existing master data...')
        logger.info('=' * 60)
        logger.info('STEP 4: Loading existing master data')
        logger.info('=' * 60)

        generator = CFXPPFileGenerator(column_order=mapper.column_order)
        generator.timestamp = timestamp
        existing_df = generator.load_master_data(batch_id)

        if existing_df.empty:
            print('  No existing master — creating new output')
        else:
            print(f'  Existing master has {len(existing_df)} rows')

        # =================================================================
        # STEP 6: Map Parsed Results to Output Columns
        # =================================================================
        print('\nSTEP 5: Mapping data to output columns...')
        logger.info('=' * 60)
        logger.info('STEP 5: Mapping parsed data to columns')
        logger.info('=' * 60)

        all_updates = {}
        skipped_count = 0
        for result in parsed_results:
            if result['file_type'] == config.FILE_TYPE_FX_PAIR:
                if result.get('client_code') == 'UNKNOWN':
                    raw = result.get('client_types_raw', '')
                    reason = f'Unknown client type: {raw}'
                    tracker.register_skipped(result, reason)
                    skipped_count += 1
                    continue
                updates = mapper.map_fx_pair_result(result)
            elif result['file_type'] == config.FILE_TYPE_CCY_POS:
                if result.get('client_code') == 'UNKNOWN':
                    raw = result.get('client_types_raw', '')
                    reason = f'Unknown client type: {raw}'
                    tracker.register_skipped(result, reason)
                    skipped_count += 1
                    continue
                updates = mapper.map_ccy_pos_result(result)
            else:
                raw = result.get('client_types_raw', '')
                reason = f'Unknown file type: {result["file_type"]}'
                tracker.register_skipped(result, reason)
                skipped_count += 1
                continue

            # Merge into all_updates
            for code, date_vals in updates.items():
                if code not in all_updates:
                    all_updates[code] = {}
                all_updates[code].update(date_vals)

            # Register with tracker
            tracker.register_processed(result, updates)

        unique_codes_updated = len(all_updates)
        total_cells = sum(len(dv) for dv in all_updates.values())
        print(f'  Columns updated: {unique_codes_updated}/540')
        print(f'  Total cell values: {total_cells}')
        if skipped_count:
            print(f'  Skipped files:   {skipped_count} (unknown client types)')

        # =================================================================
        # STEP 7: Build Output DataFrame
        # =================================================================
        print('\nSTEP 6: Building output DataFrame...')
        logger.info('=' * 60)
        logger.info('STEP 6: Building output DataFrame')
        logger.info('=' * 60)

        combined_df = build_output_dataframe(existing_df, all_updates, mapper, batch_dates)
        print(f'  Output shape: {combined_df.shape[0]} rows x {combined_df.shape[1]} cols')

        # =================================================================
        # STEP 8: Generate Output Files
        # =================================================================
        print('\nSTEP 7: Generating output files...')
        logger.info('=' * 60)
        logger.info('STEP 7: Generating output files')
        logger.info('=' * 60)

        result = generator.generate_files(combined_df, batch_id)

        # =================================================================
        # STEP 9: Save Coverage Report
        # =================================================================
        print('\nSTEP 8: Saving reports...')
        coverage_csv = os.path.join(result['output_dir'], 'coverage_report.csv')
        tracker.save_coverage_csv(coverage_csv)

        processed_csv = os.path.join(result['output_dir'], 'processed_files.csv')
        tracker.save_processed_files_csv(processed_csv)

        archive_batch_dir = os.path.join(config.ARCHIVE_DIR, batch_id)

        mapping_csv = os.path.join(result['output_dir'], 'source_column_mapping.csv')
        tracker.save_mapping_report_csv(mapping_csv, archive_dir=archive_batch_dir)

        skipped_csv = os.path.join(result['output_dir'], 'skipped_files.csv')
        tracker.save_skipped_files_csv(skipped_csv, archive_dir=archive_batch_dir)

        print(f'  Coverage report:    {os.path.basename(coverage_csv)}')
        print(f'  Mapping report:     {os.path.basename(mapping_csv)}')
        if tracker.skipped_files:
            print(f'  Skipped files:      {os.path.basename(skipped_csv)} ({len(tracker.skipped_files)} files)')

        # =================================================================
        # STEP 10: Archive Processed Files
        # =================================================================
        print('\nSTEP 9: Archiving processed files...')
        logger.info('=' * 60)
        logger.info('STEP 9: Archiving processed files')
        logger.info('=' * 60)

        file_paths = [r['file_path'] for r in parsed_results]
        archived, failed = archiver.archive_batch(file_paths, batch_id)
        print(f'  Archived: {archived} files')
        if failed:
            print(f'  Archive failures: {failed}')

        # =================================================================
        # Summary
        # =================================================================
        print('\n' + '=' * 60)
        print('EXECUTION SUMMARY')
        print('=' * 60)
        print(f"\n  Batch ID:         {batch_id}")
        print(f"  Date Range:       {range_start} to {range_end}")
        print(f"  Files Parsed:     {len(parsed_results)} "
              f"({fx_count} FX + {ccy_count} CCY)")
        if skipped_count:
            print(f"  Files Skipped:    {skipped_count}")
        if errors:
            print(f"  Parse Errors:     {len(errors)}")
        print(f"  Output Shape:     {combined_df.shape[0]} rows x {combined_df.shape[1]} cols")

        tracker.print_summary()

        print(f"\n  Files Created:")
        print(f"    DATA: {os.path.basename(result['data_file'])}")
        print(f"    META: {os.path.basename(result['meta_file'])}")
        print(f"    ZIP:  {os.path.basename(result['zip_file'])}")
        print(f"\n  Output:   {result['output_dir']}")
        print(f"  Latest:   {os.path.join(config.OUTPUT_DIR, config.LATEST_FOLDER)}")
        print(f"  Master:   {result['master_file']}")
        print(f"  Coverage: {coverage_csv}")
        print(f"  Mapping:  {mapping_csv}")
        print(f"  Archive:  {os.path.join(config.ARCHIVE_DIR, batch_id)}")
        print('\n' + '=' * 60)
        print('COMPLETED SUCCESSFULLY')
        print('=' * 60)

        logger.info('Pipeline completed successfully')
        return 0

    except KeyboardInterrupt:
        logger.warning('Execution interrupted by user')
        print('\n\nExecution interrupted by user')
        return 130

    except Exception as e:
        logger.exception(f'Pipeline failed: {e}')
        print(f'\nERROR: {e}')
        print(f'See log file for details: {log_file}')
        return 1


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
