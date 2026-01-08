"""
Utility for batching bronze layer JSON records by source table (PDF)
"""

import logging
from typing import Dict, List

logger = logging.getLogger(__name__)


def batch_by_table(bronze_data: List[dict]) -> Dict[str, List[dict]]:
    """
    Group bronze layer records by their source_table field

    Args:
        bronze_data: List of bronze layer records (flat objects from PDF tables)

    Returns:
        Dictionary mapping table names to lists of records
        Example: {"table_0": [{...}, {...}], "table_5": [{...}]}
    """
    tables = {}

    for record in bronze_data:
        table = record.get('source_table', 'Unknown')

        if table not in tables:
            tables[table] = []

        tables[table].append(record)

    logger.info(f"Batched {len(bronze_data)} records into {len(tables)} tables")
    for table_name, records in tables.items():
        logger.info(f"  - {table_name}: {len(records)} records")

    return tables


def get_table_stats(tables: Dict[str, List[dict]]) -> dict:
    """
    Get statistics about batched tables

    Args:
        tables: Dictionary from batch_by_table()

    Returns:
        Statistics dictionary with table counts and record counts
    """
    return {
        "total_tables": len(tables),
        "total_records": sum(len(records) for records in tables.values()),
        "tables": [
            {
                "name": name,
                "record_count": len(records)
            }
            for name, records in tables.items()
        ]
    }


def batch_large_table(records: List[dict], batch_size: int = 30) -> List[List[dict]]:
    """
    Split a large table into smaller batches for processing

    Args:
        records: List of records from a table
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


def batch_raw_docling_tables(docling_data: dict) -> Dict[str, dict]:
    """
    Batch raw Docling PDF tables for individual processing

    Converts raw Docling format into table batches while preserving cell structure.
    This enables table-by-table processing, classification, and batching while
    maintaining the rich cell metadata (positions, spans, headers).

    Args:
        docling_data: Raw Docling dict with format:
            {
                "source_file": "filename.pdf",
                "source_type": "pdf",
                "tables": [
                    {
                        "table_id": 0,
                        "cells": [
                            {
                                "text": "...",
                                "row": 0,
                                "col": 0,
                                "row_span": 1,
                                "col_span": 1,
                                "is_column_header": true/false,
                                "is_row_header": true/false
                            },
                            ...
                        ]
                    },
                    ...
                ]
            }

    Returns:
        Dictionary mapping table names to table data with cells:
        {
            "table_0": {"table_id": 0, "cells": [...]},
            "table_1": {"table_id": 1, "cells": [...]},
            ...
        }
    """
    tables = {}

    for table in docling_data.get('tables', []):
        table_id = table.get('table_id', 'unknown')
        table_name = f"table_{table_id}"

        # Store entire table structure with cells
        tables[table_name] = table

    logger.info(f"Batched {len(tables)} Docling tables from PDF: {docling_data.get('source_file', 'unknown')}")
    for table_name, table_data in tables.items():
        num_cells = len(table_data.get('cells', []))
        logger.info(f"  - {table_name}: {num_cells} cells")

    return tables
