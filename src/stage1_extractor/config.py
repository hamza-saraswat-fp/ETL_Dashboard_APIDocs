"""
Configuration for Excel Extractor
Simple and focused: just header detection keywords
"""

# Keywords to look for when identifying header rows
# These are common column names in HVAC catalogs
HEADER_KEYWORDS = [
    'model',
    'price',
    'cost',
    'ton',
    'tonnage',
    'seer',
    'btu',
    'outdoor',
    'indoor',
    'furnace',
    'coil',
    'evap',
    'evaporator',
    'ahri',
    'description',
    'qty',
    'quantity',
]

# Minimum number of keywords that must be present in a row
# to consider it a valid header row
MIN_KEYWORD_MATCHES = 2

# Maximum number of rows to scan for header
# (most catalogs have headers within first 20 rows)
MAX_HEADER_SCAN_ROWS = 20

# Multi-section sheet handling
# Delimiter between sheet name and section name (e.g., "GOODMAN SEER2::GAS_SYSTEMS")
SECTION_DELIMITER = "::"

# Minimum rows between headers to consider them separate sections
# (avoids false positives on data rows that happen to contain keywords)
MIN_SECTION_GAP = 3
