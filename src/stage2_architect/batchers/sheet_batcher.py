"""
Utility for batching bronze layer JSON records by source sheet
"""

import logging
from typing import Dict, List

logger = logging.getLogger(__name__)


def batch_by_sheet(bronze_data: List[dict]) -> Dict[str, List[dict]]:
    """
    Group bronze layer records by their source_sheet field

    Args:
        bronze_data: List of bronze layer records (flat objects from Excel rows)

    Returns:
        Dictionary mapping sheet names to lists of records
        Example: {"Single Stage Cooling": [{...}, {...}], "Connect": [{...}]}
    """
    sheets = {}

    for record in bronze_data:
        sheet = record.get('source_sheet', 'Unknown')

        if sheet not in sheets:
            sheets[sheet] = []

        sheets[sheet].append(record)

    logger.info(f"Batched {len(bronze_data)} records into {len(sheets)} sheets")
    for sheet_name, records in sheets.items():
        logger.info(f"  - {sheet_name}: {len(records)} records")

    return sheets


def get_sheet_stats(sheets: Dict[str, List[dict]]) -> dict:
    """
    Get statistics about batched sheets

    Args:
        sheets: Dictionary from batch_by_sheet()

    Returns:
        Statistics dictionary with sheet counts and record counts
    """
    return {
        "total_sheets": len(sheets),
        "total_records": sum(len(records) for records in sheets.values()),
        "sheets": [
            {
                "name": name,
                "record_count": len(records)
            }
            for name, records in sheets.items()
        ]
    }


def batch_large_sheet(records: List[dict], batch_size: int = 30) -> List[List[dict]]:
    """
    Split a large sheet into smaller batches for processing

    Args:
        records: List of records from a sheet
        batch_size: Maximum records per batch (default 30)

    Returns:
        List of batches, where each batch is a list of records
        Example: 66 records with batch_size=30 → [[30 records], [30 records], [6 records]]
    """
    if len(records) <= batch_size:
        return [records]  # No need to batch

    batches = []
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        batches.append(batch)

    logger.info(f"Split {len(records)} records into {len(batches)} batches of up to {batch_size} records")

    return batches
