"""
Sheet classifier - Determines if a sheet should be processed or skipped

Filters out:
- Pricing/cost sheets (just model + price)
- Table of contents / navigation
- Reference data / warranty info
- Standalone accessories

Processes:
- Full system configurations (AC, HP, Package)
- Ductless/mini-split systems
- Multi-component sheets with efficiency ratings
"""

import logging
from typing import List, Dict, Tuple

logger = logging.getLogger(__name__)

# Configuration
SKIP_NAME_PATTERNS = [
    'dealer cost', 'pricing', 'price list', 'cost sheet', 'net price', 'msrp',
    'toc', 'table of contents', 'index', 'contents', 'menu',
    'warranty', 'terms', 'terms and conditions', 'notes', 'instructions',
    'ahri reference only', 'ahri ref only', 'reference only',
    'accessories only', 'parts only', 'filters only', 'line sets only',
    'pads only', 'stands only', 'brackets only'
]

# Indicators that suggest this is a system sheet (for name-based filtering)
SYSTEM_SHEET_PATTERNS = [
    'system', 'ac ', 'air conditioning', 'cooling', 'heating',
    'heat pump', 'hp ', 'hspf',
    'ductless', 'mini split', 'multi zone', 'vrf', 'vrv',
    'package', 'packaged', 'rtu', 'rooftop',
    'single stage', 'two stage', 'variable', 'modulating',
    'connect', 'communicating', 'smart'
]

# Keys that MUST exist (with non-null values) in system configuration sheets
SYSTEM_INDICATOR_KEYS = [
    # Efficiency ratings
    'seer', 'seer2', 'eer', 'eer2', 'hspf', 'hspf2', 'afue', 'cop',
    # Capacity
    'tonnage', 'ton', 'tons', 'btu', 'capacity', 'cap',
    # Certification
    'ahri', 'ahri ref', 'ahri #',
    # System-level pricing
    'system cost', 'total price', 'system price'
]

MIN_INDICATORS_FOR_SYSTEM = 3  # Need at least 3 populated indicators to be a system sheet


def should_skip_sheet(sheet_name: str, records: List[dict]) -> Tuple[bool, str]:
    """
    Determine if a sheet should be skipped

    Args:
        sheet_name: Name of the sheet
        records: List of records from this sheet

    Returns:
        Tuple of (should_skip: bool, reason: str)
    """
    # Check name-based patterns first (fast)
    name_skip, name_reason = _check_name_patterns(sheet_name)
    if name_skip:
        return True, name_reason

    # Check structure (content analysis)
    structure_skip, structure_reason = _check_structure(sheet_name, records)

    # Return structure result (either skip or process with detailed reason)
    return structure_skip, structure_reason


def _check_name_patterns(sheet_name: str) -> Tuple[bool, str]:
    """Check if sheet name matches skip patterns"""
    name_lower = sheet_name.lower().strip()

    # Check skip patterns
    for pattern in SKIP_NAME_PATTERNS:
        if pattern in name_lower:
            return True, f"Sheet name contains '{pattern}' (skip pattern)"

    # Check if name suggests it's a system sheet (strong signal to process)
    for pattern in SYSTEM_SHEET_PATTERNS:
        if pattern in name_lower:
            logger.debug(f"Sheet '{sheet_name}' matches system pattern: '{pattern}'")
            return False, f"Sheet name suggests system data"

    # Name is neutral - continue to structure check
    return False, ""


def _count_populated_indicators(records: List[dict]) -> int:
    """
    Count how many system indicator keys have non-null values in the records.

    Also checks first 10 record VALUES for indicators (handles header-as-values pattern).

    Args:
        records: List of records from the sheet

    Returns:
        Count of unique indicators that have non-null values
    """
    if not records:
        return 0

    # Track which indicators we've found (to avoid double-counting)
    found_indicators = set()

    # NEW: Check first 10 records' VALUES for indicator keywords (header-as-values pattern)
    # This handles catalogs where header row is buried deeper in the sheet
    # Only count short values (likely headers) to avoid matching narrative text
    header_search_size = min(10, len(records))
    for record in records[:header_search_size]:
        for key, value in record.items():
            if isinstance(value, str):
                value_lower = value.lower().strip()
                # Only check values that look like headers (short, < 30 chars)
                # This avoids matching narrative text like "AHRI SYSTEM SELECTION TOOL"
                if len(value_lower) < 30:
                    for indicator in SYSTEM_INDICATOR_KEYS:
                        if indicator in value_lower:
                            found_indicators.add(indicator)
                            break

    # EXISTING: Check key names for indicators across multiple records
    sample_size = min(5, len(records))
    sample_records = records[:sample_size]

    for record in sample_records:
        for key, value in record.items():
            # Skip if value is null/empty
            if value in [None, '', 'nan', 'None', 'N/A', 'n/a']:
                continue

            # Check if this key matches any indicator
            key_lower = str(key).lower().strip()
            for indicator in SYSTEM_INDICATOR_KEYS:
                if indicator in key_lower:
                    found_indicators.add(indicator)
                    break

    return len(found_indicators)


def _check_structure(sheet_name: str, records: List[dict]) -> Tuple[bool, str]:
    """Check sheet structure to determine if it's system data"""
    if not records or len(records) == 0:
        return True, "Sheet is empty"

    # Count system indicators with non-null values
    indicator_count = _count_populated_indicators(records)

    # If we have enough indicators, this is a system sheet
    if indicator_count >= MIN_INDICATORS_FOR_SYSTEM:
        logger.debug(f"Sheet '{sheet_name}' has {indicator_count} system indicators")
        return False, f"Has {indicator_count} system indicators"

    # Calculate overall data density
    sample_size = min(5, len(records))
    sample_records = records[:sample_size]
    non_null_ratio = _calculate_non_null_ratio(sample_records)

    # Skip if mostly empty (catches Equipment Sheet, Dealer Cost, etc.)
    if non_null_ratio < 0.15:
        return True, f"Mostly empty ({non_null_ratio:.0%} non-null) - likely reference sheet"

    # Fallback: High data density likely means real data with bad headers
    # Process anyway and let LLM handle it
    if non_null_ratio >= 0.30:
        logger.warning(f"Sheet '{sheet_name}' has only {indicator_count} indicators but {non_null_ratio:.0%} data density - processing anyway (possible header detection issue)")
        return False, f"High data density ({non_null_ratio:.0%}) - processing despite {indicator_count} indicators"

    # Default: SKIP if insufficient indicators and medium-low density
    logger.debug(f"Sheet '{sheet_name}' only has {indicator_count} system indicators")
    return True, f"Only {indicator_count} system indicators found (need {MIN_INDICATORS_FOR_SYSTEM})"


def _calculate_non_null_ratio(records: List[dict]) -> float:
    """Calculate ratio of non-null values in records"""
    total_cells = 0
    non_null_cells = 0

    for record in records:
        for key, value in record.items():
            if key == 'source_sheet':
                continue  # Don't count source_sheet

            total_cells += 1

            # Count as non-null if value exists and isn't empty/placeholder
            if value is not None and value != '' and str(value).lower() not in ['nan', 'n/a', 'null']:
                non_null_cells += 1

    if total_cells == 0:
        return 0.0

    return non_null_cells / total_cells


def classify_sheets(sheets_dict: Dict[str, List[dict]]) -> Dict[str, Dict]:
    """
    Classify all sheets and return classification results

    Args:
        sheets_dict: Dictionary of {sheet_name: [records]}

    Returns:
        Dictionary of {sheet_name: {skip: bool, reason: str, record_count: int}}
    """
    results = {}

    for sheet_name, records in sheets_dict.items():
        should_skip, reason = should_skip_sheet(sheet_name, records)

        results[sheet_name] = {
            'skip': should_skip,
            'reason': reason,
            'record_count': len(records)
        }

        if should_skip:
            logger.info(f"⏭️  SKIP: {sheet_name} ({len(records)} records) - {reason}")
        else:
            logger.info(f"✅ PROCESS: {sheet_name} ({len(records)} records) - {reason}")

    return results
