"""
Table classifier - Determines if a PDF table should be processed or skipped

Filters out:
- TOC tables (table of contents)
- Header/footer tables (very small)
- Price-only tables (just model + price)
- Sparse/empty tables
- Reference tables

Processes:
- Full system configuration tables
- Equipment pricing with specifications
- Multi-column system data with efficiency ratings
"""

import logging
from typing import List, Dict, Tuple

logger = logging.getLogger(__name__)

# Minimum rows for a table to be considered (skip header/footer tables)
MIN_TABLE_ROWS = 3

# Configuration - patterns in table data that suggest skipping
SKIP_TABLE_PATTERNS = [
    # TOC indicators
    'equipment pricing summary', 'table of contents', 'page', 'contents',
    # Small reference tables
    'highlights and usage tips', 'notes', 'pricebook plus',
    # Navigation
    'index', 'menu'
]

# Indicators that suggest this is a system table
SYSTEM_TABLE_PATTERNS = [
    'system', 'ac ', 'air conditioning', 'cooling', 'heating',
    'heat pump', 'hp ', 'hspf',
    'ductless', 'mini split', 'multi zone', 'vrf', 'vrv',
    'package', 'packaged', 'rtu', 'rooftop',
    'air handler', 'furnace', 'condenser', 'evaporator'
]

# Keys that MUST exist (with non-null values) in system configuration tables
SYSTEM_INDICATOR_KEYS = [
    # Efficiency ratings
    'seer', 'seer2', 'eer', 'eer2', 'hspf', 'hspf2', 'afue', 'cop',
    # Capacity
    'tonnage', 'ton', 'tons', 'btu', 'capacity', 'cap',
    # Certification
    'ahri', 'ahri ref', 'ahri #',
    # System-level pricing
    'system cost', 'total price', 'system price', 'price',
    # Model identifiers
    'model', 'odu', 'idu', 'outdoor', 'indoor'
]

MIN_INDICATORS_FOR_SYSTEM = 2  # Need at least 2 populated indicators for PDF tables


def should_skip_table(table_name: str, records: List[dict]) -> Tuple[bool, str]:
    """
    Determine if a PDF table should be skipped

    Args:
        table_name: Name/identifier of the table (e.g., "table_0")
        records: List of records from this table

    Returns:
        Tuple of (should_skip: bool, reason: str)
    """
    # Check size first (very fast)
    if len(records) < MIN_TABLE_ROWS:
        return True, f"Too small ({len(records)} rows, need {MIN_TABLE_ROWS}+)"

    # Check pattern matching in table data
    pattern_skip, pattern_reason = _check_data_patterns(records)
    if pattern_skip:
        return True, pattern_reason

    # Check structure (content analysis)
    structure_skip, structure_reason = _check_structure(table_name, records)

    return structure_skip, structure_reason


def _check_data_patterns(records: List[dict]) -> Tuple[bool, str]:
    """Check if table data matches skip patterns"""
    # Sample first few rows to check for TOC/reference patterns
    sample_size = min(5, len(records))
    sample_records = records[:sample_size]

    for record in sample_records:
        for key, value in record.items():
            if key == 'source_table':
                continue

            # Check both keys and values for skip patterns
            combined = f"{key} {value}".lower()

            for pattern in SKIP_TABLE_PATTERNS:
                if pattern in combined:
                    return True, f"Contains '{pattern}' (skip pattern)"

            # Check for system patterns (signals to process)
            for pattern in SYSTEM_TABLE_PATTERNS:
                if pattern in combined:
                    logger.debug(f"Table matches system pattern: '{pattern}'")
                    return False, ""

    # No pattern match - continue to structure check
    return False, ""


def _count_populated_indicators(records: List[dict]) -> int:
    """
    Count how many system indicator keys have non-null values in the records.

    Also checks first 10 record VALUES for indicators (handles header-as-values pattern).

    Args:
        records: List of records from the table

    Returns:
        Count of unique indicators that have non-null values
    """
    if not records:
        return 0

    # Track which indicators we've found
    found_indicators = set()

    # Check first 10 records' VALUES for indicator keywords (header-as-values pattern)
    header_search_size = min(10, len(records))
    for record in records[:header_search_size]:
        for key, value in record.items():
            if isinstance(value, str):
                value_lower = value.lower().strip()
                # Only check values that look like headers (short, < 30 chars)
                if len(value_lower) < 30:
                    for indicator in SYSTEM_INDICATOR_KEYS:
                        if indicator in value_lower:
                            found_indicators.add(indicator)
                            break

    # Check key names for indicators across multiple records
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


def _check_structure(table_name: str, records: List[dict]) -> Tuple[bool, str]:
    """Check table structure to determine if it's system data"""
    if not records or len(records) == 0:
        return True, "Table is empty"

    # Count system indicators with non-null values
    indicator_count = _count_populated_indicators(records)

    # If we have enough indicators, this is a system table
    if indicator_count >= MIN_INDICATORS_FOR_SYSTEM:
        logger.debug(f"Table '{table_name}' has {indicator_count} system indicators")
        return False, f"Has {indicator_count} system indicators"

    # Calculate overall data density
    sample_size = min(5, len(records))
    sample_records = records[:sample_size]
    non_null_ratio = _calculate_non_null_ratio(sample_records)

    # Skip if mostly empty (catches sparse tables)
    if non_null_ratio < 0.15:
        return True, f"Too sparse ({non_null_ratio:.0%} non-null) - likely reference/TOC table"

    # Fallback: High data density likely means real data
    # Process anyway and let LLM handle it
    if non_null_ratio >= 0.30:
        logger.warning(f"Table '{table_name}' has only {indicator_count} indicators but {non_null_ratio:.0%} data density - processing anyway")
        return False, f"High data density ({non_null_ratio:.0%}) - processing despite {indicator_count} indicators"

    # Default: SKIP if insufficient indicators and medium-low density
    logger.debug(f"Table '{table_name}' only has {indicator_count} system indicators")
    return True, f"Only {indicator_count} system indicators found (need {MIN_INDICATORS_FOR_SYSTEM})"


def _calculate_non_null_ratio(records: List[dict]) -> float:
    """Calculate ratio of non-null values in records"""
    total_cells = 0
    non_null_cells = 0

    for record in records:
        for key, value in record.items():
            if key == 'source_table':
                continue  # Don't count source_table

            total_cells += 1

            # Count as non-null if value exists and isn't empty/placeholder
            if value is not None and value != '' and str(value).lower() not in ['nan', 'n/a', 'null']:
                non_null_cells += 1

    if total_cells == 0:
        return 0.0

    return non_null_cells / total_cells


def _extract_cell_text(table_data: dict) -> List[str]:
    """
    Extract all cell text from Docling table structure

    Args:
        table_data: Dict with cells: {"table_id": 0, "cells": [...]}

    Returns:
        List of all cell text values
    """
    cells = table_data.get('cells', [])
    return [cell.get('text', '').strip() for cell in cells if cell.get('text')]


def _cells_to_pseudo_records(table_data: dict) -> List[dict]:
    """
    Convert Docling cell structure to pseudo-records for classification

    Creates synthetic records by grouping cells by row, useful for applying
    existing classification logic to cell-based data.

    Args:
        table_data: Dict with cells: {"table_id": 0, "cells": [...]}

    Returns:
        List of pseudo-records (one per row) with column data
    """
    cells = table_data.get('cells', [])
    if not cells:
        return []

    # Group cells by row
    rows = {}
    for cell in cells:
        row_idx = cell.get('row', 0)
        if row_idx not in rows:
            rows[row_idx] = {}

        col_idx = cell.get('col', 0)
        text = cell.get('text', '').strip()

        # Use column index as key
        rows[row_idx][f"col_{col_idx}"] = text

    # Convert to list of dicts (pseudo-records)
    return [row_data for row_data in rows.values()]


def classify_tables(tables_dict: Dict[str, any], is_docling_format: bool = False) -> Dict[str, Dict]:
    """
    Classify all PDF tables and return classification results

    Unified classifier: handles both flattened records and Docling cell format.

    Args:
        tables_dict: Dictionary mapping table names to table data:
            - Flattened: {table_name: [records]}
            - Docling: {table_name: {"table_id": 0, "cells": [...]}}
        is_docling_format: True if tables_dict contains Docling cell structures

    Returns:
        Dictionary of {table_name: {skip: bool, reason: str, record_count/cell_count: int}}
    """
    results = {}

    for table_name, table_data in tables_dict.items():
        if is_docling_format:
            # Convert cells to pseudo-records for classification
            records = _cells_to_pseudo_records(table_data)
            count = len(table_data.get('cells', []))
            count_label = "cells"
        else:
            # Legacy flattened format
            records = table_data
            count = len(records)
            count_label = "records"

        should_skip, reason = should_skip_table(table_name, records)

        results[table_name] = {
            'skip': should_skip,
            'reason': reason,
            f'{count_label}_count': count
        }

        if should_skip:
            logger.info(f"⏭️  SKIP: {table_name} ({count} {count_label}) - {reason}")
        else:
            logger.info(f"✅ PROCESS: {table_name} ({count} {count_label}) - {reason}")

    return results
