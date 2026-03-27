"""
CFXPP Column Mapper
====================
Generates the exact 540 output column definitions and maps parsed data
to the correct output column positions.

This is the most critical module for correctness — the output column codes
and their order must match the reference output exactly.
"""

import logging

import config

logger = logging.getLogger(__name__)


class CFXPPColumnMapper:
    """Generates column definitions and maps parsed results to output columns."""

    def __init__(self):
        self.column_order = self._build_column_order()
        self.code_to_index = {code: i for i, (code, _) in enumerate(self.column_order)}

    # =========================================================================
    # COLUMN ORDER BUILDING
    # =========================================================================

    def _build_column_order(self):
        """
        Build the master list of 540 (code, description) tuples in exact output order.

        Section A: G10 Currency Positioning (90 columns)
        Section B: EM Currency Positioning (72 columns)
        Section C: FX Pair Positioning (378 columns)
        Total: 540 columns
        """
        columns = []
        columns.extend(self._build_g10_columns())
        columns.extend(self._build_em_columns())
        columns.extend(self._build_fx_pair_columns())
        return columns

    def _build_g10_columns(self):
        """
        Build G10 Currency Positioning columns (90 total).

        Code pattern:
            CFXPP.CURRENCYPOSITIONING.OVERVIEWOFCUMULATIVEPOSITIONS.POSITIONS.{CLIENT}.G10.{CCY}.B
        Note: G10 includes the .POSITIONS. segment.
        """
        columns = []
        prefix = (f'{config.CODE_PREFIX}.{config.CCY_POS_SECTION}.'
                  f'{config.CCY_POS_SUBSECTION}.{config.CCY_POS_G10_SEGMENT}')

        for client_code in config.CLIENT_TYPE_ORDER:
            client_display = config.CLIENT_TYPE_DISPLAY[client_code]
            ccys = config.G10_CCY_ORDER[client_code]
            for ccy in ccys:
                code = f'{prefix}.{client_code}.G10.{ccy}.B'
                desc = (f'Currency Positioning, Overview of Cumulative Positions, '
                        f'{client_display}, G10, {ccy}')
                columns.append((code, desc))

        return columns

    def _build_em_columns(self):
        """
        Build EM Currency Positioning columns (72 total).

        Code pattern:
            CFXPP.CURRENCYPOSITIONING.OVERVIEWOFCUMULATIVEPOSITIONS.{CLIENT}.EM.{CCY}.B
        Note: EM does NOT include the .POSITIONS. segment.
        Note: BANKS_BROKER description uses "Banks, Broker, Corporate" (reference anomaly).
        """
        columns = []
        prefix = (f'{config.CODE_PREFIX}.{config.CCY_POS_SECTION}.'
                  f'{config.CCY_POS_SUBSECTION}')

        for client_code in config.CLIENT_TYPE_ORDER:
            # EM uses a different display map for BANKS_BROKER
            client_display = config.CLIENT_TYPE_DISPLAY_EM[client_code]
            ccys = config.EM_CCY_ORDER[client_code]
            for ccy in ccys:
                code = f'{prefix}.{client_code}.EM.{ccy}.B'
                desc = (f'Currency Positioning, Overview of Cumulative Positions, '
                        f'{client_display}, EM, {ccy}')
                columns.append((code, desc))

        return columns

    def _build_fx_pair_columns(self):
        """
        Build FX Pair Positioning columns (378 total).

        Code pattern:
            CFXPP.FXPAIRPOSITIONING.NETCUMULATIVEPOSITIONSOFCURRENCYPAIRS.{CLIENT}.{METRIC}.{PAIR}.D

        Known typo (must replicate):
            BANKS_BROKER + VOLUME_NORMALIZED -> BANKS_BROKERVOLUME_NORMALIZED (missing dot)
            BANKS_BROKER + CLOSING_PRICE -> BANKS_BROKER.CLOSING_PRICE (correct)

        Per pair: 9 client types x 2 metrics = 18 columns.
        21 pairs x 18 = 378 columns.
        """
        columns = []
        prefix = (f'{config.CODE_PREFIX}.{config.FX_PAIR_SECTION}.'
                  f'{config.FX_PAIR_SUBSECTION}')

        metrics = [
            (config.METRIC_VOLUME, 'Volume (normalized)'),
            (config.METRIC_PRICE, 'Closing Price'),
        ]

        for pair in config.FX_PAIR_ORDER:
            for client_code in config.CLIENT_TYPE_ORDER:
                client_display = config.CLIENT_TYPE_DISPLAY[client_code]
                for metric_code, metric_display in metrics:
                    # Handle the known BANKS_BROKER + VOLUME_NORMALIZED typo
                    if client_code == 'BANKS_BROKER' and metric_code == config.METRIC_VOLUME:
                        code = f'{prefix}.BANKS_BROKERVOLUME_NORMALIZED.{pair}.D'
                    else:
                        code = f'{prefix}.{client_code}.{metric_code}.{pair}.D'

                    desc = (f'FX Pair Positioning, Net Cumulative Positions of Currency Pairs, '
                            f'{client_display}, {metric_display}, {pair}')
                    columns.append((code, desc))

        return columns

    # =========================================================================
    # RESULT MAPPING
    # =========================================================================

    def map_fx_pair_result(self, parsed):
        """
        Map a parsed FX Pair result to output column entries.

        Args:
            parsed: Dict from parser with keys:
                currency_pair, client_code, data (list of {date, volume, closing_price})

        Returns:
            dict: {column_code: {date_str: value}}
        """
        client_code = parsed['client_code']
        pair = parsed['currency_pair']
        prefix = (f'{config.CODE_PREFIX}.{config.FX_PAIR_SECTION}.'
                  f'{config.FX_PAIR_SUBSECTION}')

        result = {}
        for metric_code, data_key in [(config.METRIC_VOLUME, 'volume'),
                                       (config.METRIC_PRICE, 'closing_price')]:
            if client_code == 'BANKS_BROKER' and metric_code == config.METRIC_VOLUME:
                code = f'{prefix}.BANKS_BROKERVOLUME_NORMALIZED.{pair}.D'
            else:
                code = f'{prefix}.{client_code}.{metric_code}.{pair}.D'

            if code not in self.code_to_index:
                logger.warning(f'Code not in column map: {code}')
                continue

            date_values = {}
            for row in parsed['data']:
                val = row.get(data_key)
                if val is not None:
                    date_values[row['date']] = val
            result[code] = date_values

        return result

    def map_ccy_pos_result(self, parsed):
        """
        Map a parsed Currency Positioning result to output column entries.

        Args:
            parsed: Dict from parser with keys:
                client_code, ccy_group, start_date, data ({currency: value})

        Returns:
            dict: {column_code: {date_str: value}}
        """
        client_code = parsed['client_code']
        ccy_group = parsed['ccy_group'].upper()
        date = parsed['start_date']

        result = {}
        for ccy, value in parsed['data'].items():
            if ccy_group == 'G10':
                code = (f'{config.CODE_PREFIX}.{config.CCY_POS_SECTION}.'
                        f'{config.CCY_POS_SUBSECTION}.{config.CCY_POS_G10_SEGMENT}.'
                        f'{client_code}.G10.{ccy}.B')
            else:
                code = (f'{config.CODE_PREFIX}.{config.CCY_POS_SECTION}.'
                        f'{config.CCY_POS_SUBSECTION}.'
                        f'{client_code}.EM.{ccy}.B')

            if code not in self.code_to_index:
                logger.warning(f'Code not in column map: {code} (ccy={ccy}, group={ccy_group})')
                continue

            result[code] = {date: value}

        return result

    # =========================================================================
    # VALIDATION
    # =========================================================================

    def validate_against_reference(self, reference_path):
        """
        Compare generated column codes against a reference output file.

        Args:
            reference_path: Path to the reference CFXPP_DATA xlsx file.

        Returns:
            tuple: (match_count, mismatch_list)
        """
        import openpyxl

        wb = openpyxl.load_workbook(reference_path, data_only=True, read_only=True)
        ws = wb['DATA']

        ref_codes = []
        for c in range(2, 542):
            ref_codes.append(ws.cell(row=1, column=c).value)
        wb.close()

        matches = 0
        mismatches = []
        for i, (gen_code, _) in enumerate(self.column_order):
            ref_code = ref_codes[i] if i < len(ref_codes) else None
            if gen_code == ref_code:
                matches += 1
            else:
                mismatches.append({
                    'index': i,
                    'output_col': i + 2,
                    'generated': gen_code,
                    'reference': ref_code,
                })

        return matches, mismatches

    def get_column_order(self):
        """Return the column order list of (code, description) tuples."""
        return self.column_order


if __name__ == '__main__':
    mapper = CFXPPColumnMapper()
    print(f'Total columns: {len(mapper.column_order)}')
    print(f'G10 columns: 90')
    print(f'EM columns: 72')
    print(f'FX Pair columns: 378')
    print()

    # Show first few from each section
    print('=== G10 (first 3) ===')
    for code, desc in mapper.column_order[:3]:
        print(f'  {code}')
        print(f'    {desc}')

    print('\n=== EM (first 3) ===')
    for code, desc in mapper.column_order[90:93]:
        print(f'  {code}')
        print(f'    {desc}')

    print('\n=== FX Pair (first 3) ===')
    for code, desc in mapper.column_order[162:165]:
        print(f'  {code}')
        print(f'    {desc}')

    # Validate against reference if available
    import os
    ref = os.path.join(config.BASE_DIR, 'Project information', 'CFXPP_DATA_20260324.xlsx')
    if os.path.exists(ref):
        print(f'\nValidating against reference: {ref}')
        matches, mismatches = mapper.validate_against_reference(ref)
        print(f'Matches: {matches}/540')
        if mismatches:
            print(f'Mismatches: {len(mismatches)}')
            for m in mismatches[:10]:
                print(f"  Col {m['output_col']}: GEN={m['generated']}")
                print(f"         REF={m['reference']}")
        else:
            print('PERFECT MATCH - all 540 columns verified!')
