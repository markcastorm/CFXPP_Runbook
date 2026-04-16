"""
Comparison Tool Configuration
=============================
Edit these paths before running the comparison.
"""

import os

# =================================================================
# PATHS TO COMPARE
# =================================================================

# Your pipeline output (DATA file)
OUTPUT_DATA = r'D:\Projects\SIMBA-RUNBOOKS\CFXPP_Runbook\output\20260416_145155\CFXPP_DATA_20260416_145155.xlsx'

# Manual reference file to compare against
REFERENCE_DATA = r'D:\Projects\SIMBA-RUNBOOKS\CFXPP_Runbook\CFXPP_DATA_20260416.xlsx'

# Source mapping CSV (from pipeline output folder)
MAPPING_CSV = r'D:\Projects\SIMBA-RUNBOOKS\CFXPP_Runbook\output\20260416_145155\source_column_mapping.csv'

# Archive directory for the batch
ARCHIVE_DIR = r'D:\Projects\SIMBA-RUNBOOKS\CFXPP_Runbook\archive\20260410_20260416'

# =================================================================
# OUTPUT SETTINGS
# =================================================================

# Where to save comparison report
# None = save in compair directory (recommended)
# Or specify full path to save elsewhere
REPORT_OUTPUT = None

# Report filename (saved in compair directory if REPORT_OUTPUT is None)
REPORT_FILENAME = 'comparison_report.csv'

# Summary filename
SUMMARY_FILENAME = 'comparison_summary.txt'

# Tolerance for floating point comparison
FLOAT_TOLERANCE = 0.001

# =================================================================
# DISPLAY SETTINGS
# =================================================================

# Show detailed progress during comparison
VERBOSE = True

# Maximum number of examples to show in console summary
MAX_CONSOLE_EXAMPLES = 5
