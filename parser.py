"""
CFXPP Data Parser
==================
The brain of the pipeline. Dynamically scans input sheets to classify
file type and extract positioning data.

Handles two distinct file structures:
1. FX Pair Positioning - daily time series with Volume + Closing Price
2. Currency Positioning - single-date snapshots with currency-level positioning

Design: NEVER hardcode row/column positions. Always scan and detect dynamically.
"""

import os
import re
import logging
from datetime import datetime

import config
from data_loader import load_file_raw

logger = logging.getLogger(__name__)


class CFXPPParser:
    """Dynamically parses Barclays FX export files."""

    # =========================================================================
    # FILE CLASSIFICATION
    # =========================================================================

    def classify_file(self, data):
        """
        Determine file type by scanning for identifying labels.

        Args:
            data: 2D list of cell values.

        Returns:
            str: FILE_TYPE_FX_PAIR, FILE_TYPE_CCY_POS, or FILE_TYPE_UNKNOWN.
        """
        has_currency_pair = False
        has_ccy_group = False

        for row in data[:12]:
            for cell in row:
                if cell is None:
                    continue
                cell_str = str(cell).strip().lower()
                if cell_str == 'currency pair':
                    has_currency_pair = True
                elif cell_str == 'ccy group':
                    has_ccy_group = True

        if has_currency_pair:
            return config.FILE_TYPE_FX_PAIR
        elif has_ccy_group:
            return config.FILE_TYPE_CCY_POS
        else:
            return config.FILE_TYPE_UNKNOWN

    # =========================================================================
    # DYNAMIC SCANNING HELPERS
    # =========================================================================

    def _find_label_value(self, data, label, max_rows=15):
        """
        Scan cells for a label and return the value from the adjacent cell.

        Args:
            data: 2D list of cell values.
            label: Label text to search for (case-insensitive).
            max_rows: How many rows to scan.

        Returns:
            Value from the cell to the right of the label, or None.
        """
        target = label.lower().strip()
        for row_idx in range(min(max_rows, len(data))):
            row = data[row_idx]
            for col_idx, cell in enumerate(row):
                if cell is None:
                    continue
                if str(cell).strip().lower() == target:
                    # Return value from next column
                    if col_idx + 1 < len(row):
                        val = row[col_idx + 1]
                        if val is not None:
                            return val
                    # Also check the column after that (some files skip a col)
                    if col_idx + 2 < len(row):
                        val = row[col_idx + 2]
                        if val is not None:
                            return val
        return None

    def _find_section_header(self, data, header_text, start_row=0):
        """
        Scan cells to find a section header.

        Args:
            data: 2D list.
            header_text: Text to match (case-insensitive contains).
            start_row: Row to start scanning from.

        Returns:
            (row_index, col_index) or None.
        """
        target = header_text.lower().strip()
        for row_idx in range(start_row, len(data)):
            row = data[row_idx]
            for col_idx, cell in enumerate(row):
                if cell is None:
                    continue
                if target in str(cell).strip().lower():
                    return (row_idx, col_idx)
        return None

    def _find_column_headers(self, data, start_row, expected_headers):
        """
        Find the row containing expected column headers and return their positions.

        Args:
            data: 2D list.
            start_row: Row to start scanning from.
            expected_headers: List of header strings to find.

        Returns:
            dict: {header_name: col_index} or None if not found.
        """
        for row_idx in range(start_row, min(start_row + 5, len(data))):
            row = data[row_idx]
            found = {}
            for col_idx, cell in enumerate(row):
                if cell is None:
                    continue
                cell_str = str(cell).strip().lower()
                for header in expected_headers:
                    if header.lower() in cell_str:
                        found[header] = col_idx
            if len(found) >= len(expected_headers) - 1:
                return found
        return None

    def _parse_date(self, value):
        """
        Parse a date value into YYYY-MM-DD string.

        Handles: datetime objects, MM/DD/YYYY strings, YYYY-MM-DD strings.
        """
        if value is None:
            return None

        if isinstance(value, datetime):
            return value.strftime('%Y-%m-%d')

        text = str(value).strip()

        # YYYY-MM-DD
        if re.match(r'^\d{4}-\d{2}-\d{2}', text):
            return text[:10]

        # MM/DD/YYYY
        match = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})', text)
        if match:
            m, d, y = match.groups()
            return f'{y}-{m.zfill(2)}-{d.zfill(2)}'

        return text

    def _clean_numeric(self, value):
        """
        Convert a cell value to float.

        Returns float or None for missing/invalid data.
        """
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        if text in config.NA_INPUT_VALUES:
            return None
        try:
            return float(text)
        except ValueError:
            return None

    # =========================================================================
    # FX PAIR POSITIONING PARSER
    # =========================================================================

    def _parse_fx_pair(self, data, file_path):
        """
        Parse a Type 1 FX Pair Positioning file.

        Structure:
            Row ~4: Currency Pair = AUDUSD
            Row ~5: Client Types = Banks, Broker, ...
            Row ~6: STARTDATE = 2026-03-20
            Row ~7: ENDDATE = 2026-03-25
            Row ~11: Net Cumulative Positions of Currency Pairs
            Row ~12: Date | Time | Volume (normalized) | Closing Price
            Row 13+: data rows

        Returns:
            dict with file_type, currency_pair, client_code, start_date,
            end_date, data (list of {date, volume, closing_price}).
        """
        file_name = os.path.basename(file_path)

        # Extract metadata
        currency_pair = self._find_label_value(data, 'Currency Pair')
        client_types_raw = self._find_label_value(data, 'Client Types')
        start_date = self._find_label_value(data, 'STARTDATE')
        end_date = self._find_label_value(data, 'ENDDATE')

        if currency_pair is None:
            raise ValueError(f'Currency Pair not found in {file_name}')

        currency_pair = str(currency_pair).strip().upper()
        client_code = config.normalize_client_type(client_types_raw)

        if client_code is None:
            logger.warning(f'Unknown client type in {file_name}: "{client_types_raw}"')
            client_code = 'UNKNOWN'

        start_date = self._parse_date(start_date)
        end_date = self._parse_date(end_date)

        # Find the data section header
        section_pos = self._find_section_header(data, 'Net Cumulative Positions of Currency Pairs')
        if section_pos is None:
            # Fallback: look for the column headers directly
            section_row = 8
        else:
            section_row = section_pos[0]

        # Find column headers: Date, Volume (normalized), Closing Price
        col_map = self._find_column_headers(
            data, section_row,
            ['Date', 'Volume (normalized)', 'Closing Price']
        )

        if col_map is None:
            # Heuristic fallback: scan for 'Date' anywhere after section header
            col_map = {}
            for row_idx in range(section_row, min(section_row + 5, len(data))):
                row = data[row_idx]
                for col_idx, cell in enumerate(row):
                    if cell is None:
                        continue
                    cs = str(cell).strip().lower()
                    if cs == 'date':
                        col_map['Date'] = col_idx
                    elif 'volume' in cs:
                        col_map['Volume (normalized)'] = col_idx
                    elif 'closing' in cs or 'price' in cs:
                        col_map['Closing Price'] = col_idx

        if 'Date' not in col_map:
            raise ValueError(f'Date column not found in {file_name}')

        date_col = col_map.get('Date', 1)
        vol_col = col_map.get('Volume (normalized)', 3)
        price_col = col_map.get('Closing Price', 4)

        # Find data start row (first row after headers with a date value)
        data_start = None
        for row_idx in range(section_row + 1, len(data)):
            row = data[row_idx]
            if date_col < len(row) and row[date_col] is not None:
                parsed_date = self._parse_date(row[date_col])
                if parsed_date and re.match(r'^\d{4}-\d{2}-\d{2}', parsed_date):
                    data_start = row_idx
                    break

        if data_start is None:
            logger.warning(f'No data rows found in {file_name}')
            return {
                'file_type': config.FILE_TYPE_FX_PAIR,
                'currency_pair': currency_pair,
                'client_types_raw': str(client_types_raw).strip() if client_types_raw else '',
                'client_code': client_code,
                'start_date': start_date,
                'end_date': end_date,
                'data': [],
                'file_path': file_path,
            }

        # Extract data rows
        extracted_data = []
        for row_idx in range(data_start, len(data)):
            row = data[row_idx]
            if date_col >= len(row) or row[date_col] is None:
                break

            date_val = self._parse_date(row[date_col])
            if not date_val or not re.match(r'^\d{4}-\d{2}-\d{2}', date_val):
                break

            volume = self._clean_numeric(row[vol_col] if vol_col < len(row) else None)
            price = self._clean_numeric(row[price_col] if price_col < len(row) else None)

            extracted_data.append({
                'date': date_val,
                'volume': volume,
                'closing_price': price,
            })

        logger.debug(f'FX_PAIR {file_name}: {currency_pair} / {client_code} / '
                     f'{len(extracted_data)} rows')

        return {
            'file_type': config.FILE_TYPE_FX_PAIR,
            'currency_pair': currency_pair,
            'client_types_raw': str(client_types_raw).strip() if client_types_raw else '',
            'client_code': client_code,
            'start_date': start_date,
            'end_date': end_date,
            'data': extracted_data,
            'file_path': file_path,
        }

    # =========================================================================
    # CURRENCY POSITIONING PARSER
    # =========================================================================

    def _parse_ccy_pos(self, data, file_path):
        """
        Parse a Type 2 Currency Positioning file.

        Structure:
            Row ~4: Client Types = Banks, Broker, ...
            Row ~5: STARTDATE = 2026-03-24
            Row ~6: Ccy Group = G10
            Row ~9: Overview of Cumulative Positions
            Row ~10: Net Cumulative Positioning (normalized)
            Row 11+: Currency | Value (e.g., GBP -150)

        Returns:
            dict with file_type, client_code, ccy_group, start_date,
            data ({currency: value}).
        """
        file_name = os.path.basename(file_path)

        # Extract metadata
        client_types_raw = self._find_label_value(data, 'Client Types')
        start_date = self._find_label_value(data, 'STARTDATE')
        ccy_group = self._find_label_value(data, 'Ccy Group')

        if ccy_group is None:
            raise ValueError(f'Ccy Group not found in {file_name}')

        ccy_group = str(ccy_group).strip().upper()
        client_code = config.normalize_client_type(client_types_raw)
        start_date = self._parse_date(start_date)

        if client_code is None:
            logger.warning(f'Unknown client type in {file_name}: "{client_types_raw}"')
            client_code = 'UNKNOWN'

        # Find section header
        section_pos = self._find_section_header(data, 'Overview of Cumulative Positions')
        if section_pos is None:
            raise ValueError(f'Section header not found in {file_name}')

        section_row = section_pos[0]

        # Find the sub-header row with "Net Cumulative Positioning"
        value_col = None
        ccy_col = None
        data_start = None

        for row_idx in range(section_row, min(section_row + 5, len(data))):
            row = data[row_idx]
            for col_idx, cell in enumerate(row):
                if cell is None:
                    continue
                cs = str(cell).strip().lower()
                if 'net cumulative' in cs or 'positioning' in cs:
                    value_col = col_idx

        # Scan for currency data rows (3-letter currency codes with numeric values)
        extracted_data = {}
        for row_idx in range(section_row + 1, len(data)):
            row = data[row_idx]
            # Look for a cell that's a 3-letter currency code
            ccy_found = None
            val_found = None

            for col_idx, cell in enumerate(row):
                if cell is None:
                    continue
                cell_str = str(cell).strip()

                # Check if it's a currency code (2-3 uppercase letters)
                if re.match(r'^[A-Z]{2,3}$', cell_str):
                    ccy_found = cell_str
                    # Value should be in the next column
                    if col_idx + 1 < len(row):
                        val_found = self._clean_numeric(row[col_idx + 1])
                    break
                # Also handle numeric in same scan
                elif ccy_found is None and isinstance(cell, str) and len(cell.strip()) <= 3:
                    pass

            if ccy_found and ccy_found not in extracted_data:
                extracted_data[ccy_found] = val_found

        logger.debug(f'CCY_POS {file_name}: {ccy_group} / {client_code} / '
                     f'{len(extracted_data)} currencies')

        return {
            'file_type': config.FILE_TYPE_CCY_POS,
            'client_types_raw': str(client_types_raw).strip() if client_types_raw else '',
            'client_code': client_code,
            'ccy_group': ccy_group,
            'start_date': start_date,
            'data': extracted_data,
            'file_path': file_path,
        }

    # =========================================================================
    # MAIN PARSE ENTRY POINT
    # =========================================================================

    def parse_file(self, file_path, data):
        """
        Classify and parse a single file.

        Args:
            file_path: Path to the file (for logging).
            data: 2D list of cell values.

        Returns:
            dict with parsed results, or None on failure.
        """
        file_type = self.classify_file(data)

        if file_type == config.FILE_TYPE_FX_PAIR:
            return self._parse_fx_pair(data, file_path)
        elif file_type == config.FILE_TYPE_CCY_POS:
            return self._parse_ccy_pos(data, file_path)
        else:
            logger.warning(f'Unknown file type: {os.path.basename(file_path)}')
            return None


def parse_single_file(file_path):
    """
    Module-level function for multiprocessing: load and parse a single file.

    This function is picklable because it's at module level (not a method).
    All openpyxl objects are created, used, and closed within this function.

    Args:
        file_path: Absolute path to the Excel file.

    Returns:
        dict with parsed results, or None on failure.
    """
    loaded = load_file_raw(file_path)
    if loaded is None:
        return None

    parser = CFXPPParser()
    try:
        result = parser.parse_file(file_path, loaded['data'])
        return result
    except Exception as e:
        logger.error(f'Parse error for {os.path.basename(file_path)}: {e}')
        return None


if __name__ == '__main__':
    from logger_setup import setup_logging
    setup_logging()

    loader_module = __import__('data_loader')
    loader = loader_module.CFXPPDataLoader()
    files = loader.find_input_files()

    if files:
        print(f'Testing parser on first 3 files...\n')
        for fp in files[:3]:
            result = parse_single_file(fp)
            if result:
                print(f"File: {os.path.basename(fp)}")
                print(f"  Type: {result['file_type']}")
                if result['file_type'] == config.FILE_TYPE_FX_PAIR:
                    print(f"  Pair: {result['currency_pair']}")
                    print(f"  Client: {result['client_code']}")
                    print(f"  Dates: {result['start_date']} to {result['end_date']}")
                    print(f"  Rows: {len(result['data'])}")
                    if result['data']:
                        print(f"  Sample: {result['data'][0]}")
                elif result['file_type'] == config.FILE_TYPE_CCY_POS:
                    print(f"  Group: {result['ccy_group']}")
                    print(f"  Client: {result['client_code']}")
                    print(f"  Date: {result['start_date']}")
                    print(f"  Currencies: {len(result['data'])}")
                    print(f"  Data: {result['data']}")
                print()
