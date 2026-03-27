"""
CFXPP Runbook Configuration
============================
Central configuration for the Barclays FX Pair Positioning data processing pipeline.

This runbook processes Excel export files from Barclays containing FX positioning data
for 21 currency pairs and 18 individual currencies across 9 client type combinations.
"""

import os
from datetime import datetime

# =============================================================================
# PATHS CONFIGURATION
# =============================================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

INPUT_DIR = os.path.join(BASE_DIR, 'Input')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
MASTER_DIR = os.path.join(BASE_DIR, 'Master Data')
ARCHIVE_DIR = os.path.join(BASE_DIR, 'archive')
LOGS_DIR = os.path.join(BASE_DIR, 'logs')

# =============================================================================
# FILE TYPE IDENTIFIERS
# =============================================================================

FILE_TYPE_FX_PAIR = 'FX_PAIR'
FILE_TYPE_CCY_POS = 'CCY_POS'
FILE_TYPE_UNKNOWN = 'UNKNOWN'

# =============================================================================
# CLIENT TYPE MAPPING
# =============================================================================

# Maps raw input strings -> normalized output code
CLIENT_TYPE_MAP = {
    'Banks, Broker, Corporate, Hedge Fund, Real Money, Unclassified':
        'BANKS_BROKER_CORPORATE_HEDGEFUND_REALMONEY_UNCLASSIFIED',
    'Banks, Broker':        'BANKS_BROKER',
    'Banks, Broker, Corporate': 'BANKS_BROKER',  # EM variant maps to same code
    'Corporate, Real Money': 'CORPORATE_REALMONEY',
    'Banks':                'BANKS',
    'Broker':               'BROKER',
    'Corporate':            'CORPORATE',
    'Hedge Fund':           'HEDGEFUND',
    'Real Money':           'REALMONEY',
    'Unclassified':         'UNCLASSIFIED',
}

# Ordered list of client type codes (matches output column ordering)
CLIENT_TYPE_ORDER = [
    'BANKS_BROKER_CORPORATE_HEDGEFUND_REALMONEY_UNCLASSIFIED',
    'BANKS_BROKER',
    'CORPORATE_REALMONEY',
    'BANKS',
    'BROKER',
    'CORPORATE',
    'HEDGEFUND',
    'REALMONEY',
    'UNCLASSIFIED',
]

# Human-readable display names for descriptions
CLIENT_TYPE_DISPLAY = {
    'BANKS_BROKER_CORPORATE_HEDGEFUND_REALMONEY_UNCLASSIFIED':
        'Banks, Broker, Corporate, Hedge Fund, Real Money, Unclassified',
    'BANKS_BROKER':         'Banks, Broker',
    'CORPORATE_REALMONEY':  'Corporate, Real Money',
    'BANKS':                'Banks',
    'BROKER':               'Broker',
    'CORPORATE':            'Corporate',
    'HEDGEFUND':            'Hedge Fund',
    'REALMONEY':            'Real Money',
    'UNCLASSIFIED':         'Unclassified',
}

# EM section has a known description anomaly: BANKS_BROKER shows "Banks, Broker, Corporate"
CLIENT_TYPE_DISPLAY_EM = {
    **CLIENT_TYPE_DISPLAY,
    'BANKS_BROKER': 'Banks, Broker, Corporate',
}

# =============================================================================
# FX PAIR ORDER (21 pairs, matches reference output exactly)
# =============================================================================

FX_PAIR_ORDER = [
    'EURUSD', 'GBPUSD', 'AUDUSD', 'CHFUSD', 'USDJPY',
    'USDMXN', 'USDCAD', 'USDBRL', 'USDCLP', 'USDCOP',
    'USDPEN', 'USDTRY', 'USDZAR', 'USDPLN', 'USDCNH',
    'USDHKD', 'USDSGD', 'USDKRW', 'USDINR', 'USDTWD',
    'USDPHP',
]

# =============================================================================
# CURRENCY ORDERS (per client type — verified from reference output)
# Each client type has its own unique order; these are absolute.
# =============================================================================

G10_CCY_ORDER = {
    'BANKS_BROKER_CORPORATE_HEDGEFUND_REALMONEY_UNCLASSIFIED':
        ['USD', 'EUR', 'AUD', 'NZD', 'NOK', 'CAD', 'SEK', 'JPY', 'CHF', 'GBP'],
    'BANKS_BROKER':
        ['USD', 'EUR', 'AUD', 'NZD', 'NOK', 'CAD', 'SEK', 'JPY', 'CHF', 'GBP'],
    'CORPORATE_REALMONEY':
        ['USD', 'AUD', 'CHF', 'NOK', 'NZD', 'SEK', 'JPY', 'CAD', 'GBP', 'EUR'],
    'BANKS':
        ['JPY', 'EUR', 'CAD', 'AUD', 'GBP', 'NZD', 'NOK', 'SEK', 'CHF', 'USD'],
    'BROKER':
        ['JPY', 'EUR', 'CAD', 'AUD', 'GBP', 'NZD', 'NOK', 'SEK', 'CHF', 'USD'],
    'CORPORATE':
        ['CHF', 'JPY', 'CAD', 'NOK', 'NZD', 'SEK', 'AUD', 'GBP', 'USD', 'EUR'],
    'HEDGEFUND':
        ['USD', 'NZD', 'CAD', 'SEK', 'NOK', 'CHF', 'AUD', 'EUR', 'GBP', 'JPY'],
    'REALMONEY':
        ['USD', 'AUD', 'NOK', 'SEK', 'NZD', 'CHF', 'JPY', 'EUR', 'CAD', 'GBP'],
    'UNCLASSIFIED':
        ['GBP', 'CAD', 'AUD', 'SEK', 'CHF', 'EUR', 'NOK', 'NZD', 'JPY', 'USD'],
}

EM_CCY_ORDER = {
    'BANKS_BROKER_CORPORATE_HEDGEFUND_REALMONEY_UNCLASSIFIED':
        ['TRY', 'INR', 'MXN', 'SGD', 'PLN', 'CZK', 'HUF', 'ZAR'],
    'BANKS_BROKER':
        ['MXN', 'PLN', 'ZAR', 'HUF', 'SGD', 'CZK', 'INR', 'TRY'],
    'CORPORATE_REALMONEY':
        ['HUF', 'INR', 'CZK', 'SGD', 'PLN', 'MXN', 'ZAR', 'TRY'],
    'BANKS':
        ['MXN', 'SGD', 'PLN', 'ZAR', 'HUF', 'CZK', 'INR', 'TRY'],
    'BROKER':
        ['INR', 'PLN', 'ZAR', 'HUF', 'CZK', 'TRY', 'MXN', 'SGD'],
    'CORPORATE':
        ['TRY', 'INR', 'MXN', 'CZK', 'HUF', 'PLN', 'ZAR', 'SGD'],
    'HEDGEFUND':
        ['TRY', 'INR', 'SGD', 'CZK', 'ZAR', 'PLN', 'HUF', 'MXN'],
    'REALMONEY':
        ['SGD', 'HUF', 'PLN', 'CZK', 'MXN', 'INR', 'ZAR', 'TRY'],
    'UNCLASSIFIED':
        ['INR', 'SGD', 'CZK', 'HUF', 'TRY', 'PLN', 'MXN', 'ZAR'],
}

# =============================================================================
# CODE PREFIX CONSTANTS
# =============================================================================

CODE_PREFIX = 'CFXPP'

# Currency Positioning code components
CCY_POS_SECTION = 'CURRENCYPOSITIONING'
CCY_POS_SUBSECTION = 'OVERVIEWOFCUMULATIVEPOSITIONS'
CCY_POS_G10_SEGMENT = 'POSITIONS'  # G10 has this extra segment; EM does not

# FX Pair Positioning code components
FX_PAIR_SECTION = 'FXPAIRPOSITIONING'
FX_PAIR_SUBSECTION = 'NETCUMULATIVEPOSITIONSOFCURRENCYPAIRS'

# Metric codes for FX Pair output
METRIC_VOLUME = 'VOLUME_NORMALIZED'
METRIC_PRICE = 'CLOSING_PRICE'

# =============================================================================
# METADATA CONFIGURATION (for META sheet)
# =============================================================================

METADATA_DEFAULTS = {
    'MULTIPLIER': 0.0,
    'AGGREGATION_TYPE': 'UNDEFINED',
    'UNIT_TYPE': 'FLOW',
    'DATA_TYPE': 'INDEX',
    'DATA_UNIT': 'INDEX',
    'SEASONALLY_ADJUSTED': 'NSA',
    'ANNUALIZED': False,
    'PROVIDER_MEASURE_URL': 'https://live.barcap.com/BU/C/FXRA/app/',
    'PROVIDER': 'AfricaAI',
    'SOURCE': 'BARCLAYS',
    'SOURCE_DESCRIPTION': 'Barclays Bank',
    'COUNTRY': 'USA',
    'DATASET': 'CFXPP',
    'LAST_RELEASE_DATE': '2025-06-05T10:00:00',
}

META_HEADERS = [
    'CODE', 'CODE_MNEMONIC', 'DESCRIPTION', 'FREQUENCY',
    'MULTIPLIER', 'AGGREGATION_TYPE', 'UNIT_TYPE', 'DATA_TYPE',
    'DATA_UNIT', 'SEASONALLY_ADJUSTED', 'ANNUALIZED',
    'PROVIDER_MEASURE_URL', 'PROVIDER', 'SOURCE',
    'SOURCE_DESCRIPTION', 'COUNTRY', 'DATASET', 'LAST_RELEASE_DATE',
]

# =============================================================================
# FILE NAMING CONFIGURATION
# =============================================================================

DATA_FILE_PREFIX = 'CFXPP_DATA'
META_FILE_PREFIX = 'CFXPP_META'
ZIP_FILE_PREFIX = 'CFXPP'
TIMESTAMP_FORMAT = '%Y%m%d_%H%M%S'
LATEST_FOLDER = 'latest'

# =============================================================================
# MULTIPROCESSING CONFIGURATION
# =============================================================================

MAX_WORKERS = min(os.cpu_count() or 4, 8)

# =============================================================================
# DATA PROCESSING CONFIGURATION
# =============================================================================

NA_INPUT_VALUES = ['-', '--', 'N/A', 'NA', '', None]
CONTINUE_ON_ERROR = True

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

LOG_LEVEL = 'INFO'
DEBUG_MODE = False
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def get_timestamp():
    """Get current timestamp string for file naming."""
    return datetime.now().strftime(TIMESTAMP_FORMAT)


def get_batch_id(start_date, end_date):
    """
    Build batch identifier from start/end dates.

    Args:
        start_date: Start date string (YYYY-MM-DD).
        end_date: End date string (YYYY-MM-DD) or None.

    Returns:
        str: Batch ID like '20260320_20260325'.
    """
    sd = str(start_date).replace('-', '')
    if end_date:
        ed = str(end_date).replace('-', '')
    else:
        ed = sd
    return f'{sd}_{ed}'


def normalize_client_type(raw):
    """
    Normalize a raw client type string from source files.

    Strips whitespace, handles common variations.

    Returns:
        str: Normalized client type code, or None if not recognized.
    """
    if raw is None:
        return None
    cleaned = str(raw).strip()
    # Remove trailing comma if present
    cleaned = cleaned.rstrip(',').strip()
    if cleaned in CLIENT_TYPE_MAP:
        return CLIENT_TYPE_MAP[cleaned]
    # Case-insensitive match
    for key, code in CLIENT_TYPE_MAP.items():
        if key.lower() == cleaned.lower():
            return code
    return None


def is_skip_area(name):
    """Check if a name is a region/aggregate we should skip."""
    return False


if __name__ == '__main__':
    print('CFXPP Configuration Summary')
    print('=' * 50)
    print(f'Base Directory: {BASE_DIR}')
    print(f'Input Directory: {INPUT_DIR}')
    print(f'Output Directory: {OUTPUT_DIR}')
    print(f'Master Directory: {MASTER_DIR}')
    print(f'Archive Directory: {ARCHIVE_DIR}')
    print(f'FX Pairs: {len(FX_PAIR_ORDER)}')
    print(f'Client Types: {len(CLIENT_TYPE_ORDER)}')
    print(f'G10 Currencies: {len(G10_CCY_ORDER[CLIENT_TYPE_ORDER[0]])}')
    print(f'EM Currencies: {len(EM_CCY_ORDER[CLIENT_TYPE_ORDER[0]])}')
    print(f'Max Workers: {MAX_WORKERS}')
