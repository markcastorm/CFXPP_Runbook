"""
CFXPP Coverage Tracker
=======================
Tracks which data combinations have been processed and generates
CSV reports of coverage (what's filled vs what's missing).
"""

import os
import csv
import logging
from collections import defaultdict

import config

logger = logging.getLogger(__name__)


class CFXPPTracker:
    """Tracks processing coverage across all expected output columns."""

    def __init__(self, column_order, code_to_index=None):
        """
        Args:
            column_order: List of (code, description) tuples from column_mapper.
            code_to_index: Dict mapping column code -> 0-based index in column_order.
        """
        self.column_order = column_order
        self.code_to_index = code_to_index or {}
        self.code_to_desc = {code: desc for code, desc in column_order}
        self.processed_files = []
        self.filled_cells = defaultdict(set)  # {column_code: set of dates}
        self.all_dates = set()
        self.file_counts = {
            config.FILE_TYPE_FX_PAIR: 0,
            config.FILE_TYPE_CCY_POS: 0,
        }
        self.error_files = []
        self.file_column_mappings = []  # Detailed source-to-column mappings
        self.skipped_files = []  # Files parsed but skipped (unknown client type)

    def register_processed(self, parsed_result, mapped_updates):
        """
        Record a successfully processed file.

        Args:
            parsed_result: Dict from parser.
            mapped_updates: Dict from column_mapper {code: {date: value}}.
        """
        file_type = parsed_result.get('file_type', 'UNKNOWN')
        file_name = os.path.basename(parsed_result.get('file_path', 'unknown'))

        self.processed_files.append({
            'file_name': file_name,
            'file_type': file_type,
            'client_code': parsed_result.get('client_code', ''),
            'currency_pair': parsed_result.get('currency_pair', ''),
            'ccy_group': parsed_result.get('ccy_group', ''),
            'start_date': parsed_result.get('start_date', ''),
        })

        if file_type in self.file_counts:
            self.file_counts[file_type] += 1

        # Record detailed file-to-column mappings
        for code, date_vals in mapped_updates.items():
            for date, val in date_vals.items():
                if val is not None:
                    self.filled_cells[code].add(date)
                    self.all_dates.add(date)

            # Determine metric and currency for this column
            metric, currency = self._extract_metric_currency(code, file_type)

            self.file_column_mappings.append({
                'file_name': file_name,
                'file_type': file_type,
                'client_types_raw': parsed_result.get('client_types_raw', ''),
                'client_code': parsed_result.get('client_code', ''),
                'currency_pair': parsed_result.get('currency_pair', ''),
                'ccy_group': parsed_result.get('ccy_group', ''),
                'start_date': parsed_result.get('start_date', ''),
                'end_date': parsed_result.get('end_date', ''),
                'column_code': code,
                'column_index': self.code_to_index.get(code, -1) + 2,  # 1-based output col
                'column_desc': self.code_to_desc.get(code, ''),
                'metric': metric,
                'currency': currency,
                'date_values': date_vals,
            })

    def _extract_metric_currency(self, code, file_type):
        """Extract metric type and currency/pair from a column code."""
        if file_type == config.FILE_TYPE_FX_PAIR:
            if 'VOLUME_NORMALIZED' in code:
                metric = 'VOLUME_NORMALIZED'
            elif 'CLOSING_PRICE' in code:
                metric = 'CLOSING_PRICE'
            else:
                metric = ''
            return metric, ''
        else:
            # CCY_POS — currency is second-to-last segment
            parts = code.split('.')
            ccy = parts[-2] if len(parts) >= 2 else ''
            return 'CCY_VALUE', ccy

    def register_skipped(self, parsed_result, reason):
        """
        Record a file that was parsed but skipped (e.g., unknown client type).

        Args:
            parsed_result: Dict from parser.
            reason: String explaining why the file was skipped.
        """
        self.skipped_files.append({
            'file_name': os.path.basename(parsed_result.get('file_path', 'unknown')),
            'file_type': parsed_result.get('file_type', 'UNKNOWN'),
            'client_types_raw': parsed_result.get('client_types_raw', ''),
            'client_code': parsed_result.get('client_code', ''),
            'currency_pair': parsed_result.get('currency_pair', ''),
            'ccy_group': parsed_result.get('ccy_group', ''),
            'start_date': parsed_result.get('start_date', ''),
            'end_date': parsed_result.get('end_date', ''),
            'reason': reason,
        })

    def register_error(self, file_path, error_msg):
        """Record a file that failed processing."""
        self.error_files.append({
            'file_path': file_path,
            'file_name': os.path.basename(file_path),
            'error': str(error_msg),
        })

    def get_coverage_stats(self):
        """
        Calculate coverage statistics.

        Returns:
            dict with total_columns, filled_columns, missing_columns,
            coverage_pct, total_cells, filled_cells, etc.
        """
        total_columns = len(self.column_order)
        filled_columns = len(self.filled_cells)
        missing_columns = total_columns - filled_columns

        total_dates = len(self.all_dates)
        total_cells = total_columns * total_dates if total_dates > 0 else 0
        filled_count = sum(len(dates) for dates in self.filled_cells.values())

        return {
            'total_columns': total_columns,
            'filled_columns': filled_columns,
            'missing_columns': missing_columns,
            'column_coverage_pct': (filled_columns / total_columns * 100) if total_columns > 0 else 0,
            'total_dates': total_dates,
            'total_cells': total_cells,
            'filled_cells': filled_count,
            'cell_coverage_pct': (filled_count / total_cells * 100) if total_cells > 0 else 0,
            'files_processed': len(self.processed_files),
            'fx_pair_files': self.file_counts[config.FILE_TYPE_FX_PAIR],
            'ccy_pos_files': self.file_counts[config.FILE_TYPE_CCY_POS],
            'error_files': len(self.error_files),
        }

    def save_coverage_csv(self, output_path):
        """
        Write detailed coverage report to CSV.

        Columns: code, description, section, status, dates_found, dates_missing
        """
        logger.info(f'Saving coverage report to {output_path}')

        sorted_dates = sorted(self.all_dates)

        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            # Header
            header = ['Code', 'Description', 'Section', 'Status',
                      'Dates Found', 'Dates Missing', 'Fill Count', 'Total Dates']
            writer.writerow(header)

            for code, desc in self.column_order:
                # Determine section
                if '.CURRENCYPOSITIONING.' in code and '.G10.' in code:
                    section = 'CCY_POS_G10'
                elif '.CURRENCYPOSITIONING.' in code and '.EM.' in code:
                    section = 'CCY_POS_EM'
                elif '.FXPAIRPOSITIONING.' in code:
                    section = 'FX_PAIR'
                else:
                    section = 'OTHER'

                filled_dates = self.filled_cells.get(code, set())
                missing_dates = set(sorted_dates) - filled_dates

                if not sorted_dates:
                    status = 'NO_DATA'
                elif len(filled_dates) == len(sorted_dates):
                    status = 'COMPLETE'
                elif len(filled_dates) > 0:
                    status = 'PARTIAL'
                else:
                    status = 'MISSING'

                writer.writerow([
                    code,
                    desc,
                    section,
                    status,
                    '; '.join(sorted(filled_dates)),
                    '; '.join(sorted(missing_dates)),
                    len(filled_dates),
                    len(sorted_dates),
                ])

        # Also write error files
        if self.error_files:
            errors_path = output_path.replace('.csv', '_errors.csv')
            with open(errors_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['File', 'Error'])
                for err in self.error_files:
                    writer.writerow([err['file_name'], err['error']])
            logger.info(f'Error report saved to {errors_path}')

        logger.info(f'Coverage report saved: {len(self.column_order)} columns tracked')

    def save_processed_files_csv(self, output_path):
        """Write list of all processed files to CSV."""
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['File', 'Type', 'Client Code', 'Currency Pair',
                             'Ccy Group', 'Start Date'])
            for entry in self.processed_files:
                writer.writerow([
                    entry['file_name'],
                    entry['file_type'],
                    entry['client_code'],
                    entry['currency_pair'],
                    entry['ccy_group'],
                    entry['start_date'],
                ])

    def save_mapping_report_csv(self, output_path, archive_dir=None):
        """
        Write detailed source-to-column mapping report to CSV.

        One row per (source_file, output_column) combination, showing exactly
        which source file maps to which output column with all header metadata
        and actual values. Essential for diagnosing data mismatches.

        Args:
            output_path: Path for the CSV file.
            archive_dir: Optional archive directory path for generating file links.
        """
        logger.info(f'Saving source-column mapping report to {output_path}')

        # Build a lookup: column_code -> list of file_names that write to it
        code_to_files = defaultdict(list)
        for entry in self.file_column_mappings:
            code_to_files[entry['column_code']].append(entry['file_name'])

        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            header = [
                'Source_File',
                'Archive_Link',
                'File_Type',
                'Raw_Client_Types',
                'Normalized_Client_Code',
                'Currency_Pair',
                'Ccy_Group',
                'Start_Date',
                'End_Date',
                'Section',
                'Output_Column_Code',
                'Output_Column_Index',
                'Output_Column_Description',
                'Metric',
                'Currency',
                'Dates_With_Data',
                'Date_Values',
                'Is_Duplicate',
                'Duplicate_Files',
            ]
            writer.writerow(header)

            for entry in self.file_column_mappings:
                code = entry['column_code']

                # Determine section
                if '.CURRENCYPOSITIONING.' in code and '.G10.' in code:
                    section = 'G10_CCY_POS'
                elif '.CURRENCYPOSITIONING.' in code and '.EM.' in code:
                    section = 'EM_CCY_POS'
                elif '.FXPAIRPOSITIONING.' in code:
                    section = 'FX_PAIR'
                else:
                    section = 'OTHER'

                # Format date=value pairs
                date_vals = entry['date_values']
                non_null = {d: v for d, v in date_vals.items() if v is not None}
                date_val_str = '; '.join(
                    f'{d}={v}' for d, v in sorted(non_null.items())
                )

                # Duplicate detection
                other_files = [
                    fn for fn in code_to_files[code]
                    if fn != entry['file_name']
                ]
                is_duplicate = 'YES' if other_files else 'NO'
                dup_files = '; '.join(sorted(set(other_files)))

                # Archive link
                archive_link = ''
                if archive_dir:
                    archive_link = os.path.join(archive_dir, entry['file_name'])

                writer.writerow([
                    entry['file_name'],
                    archive_link,
                    entry['file_type'],
                    entry['client_types_raw'],
                    entry['client_code'],
                    entry['currency_pair'],
                    entry['ccy_group'],
                    entry['start_date'],
                    entry['end_date'],
                    section,
                    code,
                    entry['column_index'],
                    entry['column_desc'],
                    entry['metric'],
                    entry['currency'],
                    len(non_null),
                    date_val_str,
                    is_duplicate,
                    dup_files,
                ])

        logger.info(f'Mapping report saved: {len(self.file_column_mappings)} mappings')

    def save_skipped_files_csv(self, output_path, archive_dir=None):
        """
        Write report of files that were parsed but skipped.

        These are files with unrecognized client types or other issues
        that prevent them from mapping to output columns.

        Args:
            output_path: Path for the CSV file.
            archive_dir: Optional archive directory path for generating file links.
        """
        if not self.skipped_files:
            return

        logger.info(f'Saving skipped files report to {output_path}')

        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Source_File', 'Archive_Link', 'File_Type', 'Raw_Client_Types',
                'Normalized_Client_Code', 'Currency_Pair', 'Ccy_Group',
                'Start_Date', 'End_Date', 'Reason',
            ])
            for entry in self.skipped_files:
                archive_link = ''
                if archive_dir:
                    archive_link = os.path.join(archive_dir, entry['file_name'])
                writer.writerow([
                    entry['file_name'],
                    archive_link,
                    entry['file_type'],
                    entry['client_types_raw'],
                    entry['client_code'],
                    entry['currency_pair'],
                    entry['ccy_group'],
                    entry['start_date'],
                    entry['end_date'],
                    entry['reason'],
                ])

        logger.info(f'Skipped files report saved: {len(self.skipped_files)} files')

    def print_summary(self):
        """Print coverage summary to console."""
        stats = self.get_coverage_stats()

        print(f"\n  Coverage Summary:")
        print(f"    Files Processed:  {stats['files_processed']} "
              f"({stats['fx_pair_files']} FX Pair + {stats['ccy_pos_files']} CCY Pos)")
        if stats['error_files'] > 0:
            print(f"    Files Errored:    {stats['error_files']}")
        print(f"    Columns Filled:   {stats['filled_columns']}/{stats['total_columns']} "
              f"({stats['column_coverage_pct']:.1f}%)")
        print(f"    Dates Tracked:    {stats['total_dates']}")
        if stats['total_cells'] > 0:
            print(f"    Cells Filled:     {stats['filled_cells']}/{stats['total_cells']} "
                  f"({stats['cell_coverage_pct']:.1f}%)")
