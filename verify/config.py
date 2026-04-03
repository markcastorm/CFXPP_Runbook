"""
Source Coverage Verification Tool Configuration
================================================
Edit these paths before running the verification.
"""

import os

# =================================================================
# PATHS TO VERIFY
# =================================================================

# Archive folder containing source files to analyze
ARCHIVE_FOLDER = r'D:\Projects\SIMBA-RUNBOOKS\CFXPP_Runbook\archive\20260330_20260403'

# Output file to verify (DATA file from pipeline)
OUTPUT_FILE = r'D:\Projects\SIMBA-RUNBOOKS\CFXPP_Runbook\output\20260403_150138\CFXPP_DATA_20260403_150138.xlsx'

# Alternative: Use latest output instead of master
# OUTPUT_FILE = r'D:\Projects\SIMBA-RUNBOOKS\CFXPP_Runbook\output\latest\CFXPP_DATA_latest.xlsx'

# =================================================================
# OUTPUT SETTINGS
# =================================================================

# Where to save verification reports
# None = save in verify directory (recommended)
# Or specify full path to save elsewhere
REPORT_OUTPUT_DIR = None

# Create timestamped run folder for each verification (like compair does)
CREATE_RUN_FOLDER = True

# =================================================================
# DISPLAY SETTINGS
# =================================================================

# Show detailed progress during file analysis
VERBOSE = True

# Maximum number of file combinations to show in console
MAX_CONSOLE_EXAMPLES = 10
