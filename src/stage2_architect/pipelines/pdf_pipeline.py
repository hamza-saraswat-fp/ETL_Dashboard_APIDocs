"""
PDF Pipeline - Orchestrates PDF bronze to silver transformation

Flow:
1. Load bronze JSON (raw Docling format)
2. Batch by table (preserves cell structure)
3. Classify tables (filter non-system tables)
4. Transform each table using LLM with cell data
5. Combine and return results

Unified Pipeline: All PDFs use raw Docling format with cell structure
"""

import logging
from typing import List, Dict, Any, Union

from ..batchers import batch_by_table, get_table_stats, batch_large_table, batch_raw_docling_tables
from ..classifiers import classify_tables

logger = logging.getLogger(__name__)


def process_pdf_bronze(
    bronze_data: Union[Dict, List[dict]],
    llm_transform_fn,
    prompt_template: str
) -> Dict[str, Any]:
    """
    Process PDF bronze data through classification, batching, and LLM transformation

    Unified Pipeline: Handles raw Docling format (preferred) with cell structure.
    Legacy flattened format supported for backward compatibility.

    Args:
        bronze_data: Either:
            - Dict with raw Docling format: {source_file, source_type, tables: [{table_id, cells}]}
            - List with flattened format: [{source_table, ...}, ...] (legacy)
        llm_transform_fn: Function to call for LLM transformation
            Signature: (table_name: str, table_data: dict) -> tuple[list, dict]
            table_data is either:
                - Dict with cells: {"table_id": 0, "cells": [...]} (Docling)
                - List of records: [{...}, {...}] (flattened)
        prompt_template: Base prompt template for LLM

    Returns:
        Dictionary with:
        - systems: List of all extracted systems
        - table_results: List of per-table processing results
        - stats: Processing statistics
    """
    logger.info("=== PDF Pipeline: Starting ===")

    # Step 1: Detect format and batch by table
    is_raw_docling = isinstance(bronze_data, dict) and 'tables' in bronze_data

    if is_raw_docling:
        logger.info("Step 1: Batching raw Docling tables (cell structure preserved)")
        tables = batch_raw_docling_tables(bronze_data)
        num_tables = len(tables)
        logger.info(f"Found {num_tables} Docling tables from {bronze_data.get('source_file', 'unknown')}")
    else:
        logger.info("Step 1: Batching flattened records by source_table (legacy format)")
        tables = batch_by_table(bronze_data)
        table_stats = get_table_stats(tables)
        logger.info(f"Found {table_stats['total_tables']} tables with {table_stats['total_records']} total records")

    # Step 2: Classify tables (filter out non-system tables)
    num_tables = len(tables)
    logger.info(f"Step 2: Classifying {num_tables} tables")
    classifications = classify_tables(tables, is_docling_format=is_raw_docling)

    # Separate processable and skipped tables
    tables_to_process = {name: table_data for name, table_data in tables.items()
                        if not classifications[name]['skip']}
    tables_skipped = {name: table_data for name, table_data in tables.items()
                     if classifications[name]['skip']}

    logger.info(f"Processing {len(tables_to_process)} tables, skipping {len(tables_skipped)} tables")

    # Step 3: Transform each table using LLM
    logger.info(f"Step 3: Transforming {len(tables_to_process)} tables using LLM")
    all_systems = []
    table_results = []

    for table_name, table_data in tables_to_process.items():
        logger.info(f"\n--- Processing table: {table_name} ---")

        try:
            if is_raw_docling:
                # Raw Docling: table_data is dict with cells
                num_cells = len(table_data.get('cells', []))
                logger.info(f"Cells: {num_cells}")

                # For now, process entire table in one call
                # TODO: Add cell-based batching for very large tables
                systems, table_meta = llm_transform_fn(table_name, table_data)

                all_systems.extend(systems)
                table_results.append({
                    "table_name": table_name,
                    "input_cells": num_cells,
                    "output_systems": len(systems),
                    "batches": 1,
                    "success": True,
                    "metadata": table_meta
                })

                logger.info(f"✅ {table_name}: {num_cells} cells → {len(systems)} systems")

            else:
                # Legacy flattened: table_data is list of records
                table_records = table_data
                logger.info(f"Records: {len(table_records)}")

                # Check if table needs batching (>30 records)
                if len(table_records) > 30:
                    logger.info(f"Large table detected, splitting into batches...")
                    record_batches = batch_large_table(table_records, batch_size=30)

                    table_systems = []
                    for batch_idx, batch_records in enumerate(record_batches, 1):
                        logger.info(f"  Processing batch {batch_idx}/{len(record_batches)} ({len(batch_records)} records)")

                        batch_systems, batch_meta = llm_transform_fn(
                            f"{table_name} (batch {batch_idx})",
                            batch_records
                        )
                        table_systems.extend(batch_systems)
                        logger.info(f"  ✅ Batch {batch_idx}: {len(batch_records)} records → {len(batch_systems)} systems")

                    all_systems.extend(table_systems)
                    table_results.append({
                        "table_name": table_name,
                        "input_records": len(table_records),
                        "output_systems": len(table_systems),
                        "batches": len(record_batches),
                        "success": True
                    })

                    logger.info(f"✅ {table_name}: {len(table_records)} records → {len(table_systems)} systems (via {len(record_batches)} batches)")

                else:
                    # Process entire table in one call
                    systems, table_meta = llm_transform_fn(table_name, table_records)

                    all_systems.extend(systems)
                    table_results.append({
                        "table_name": table_name,
                        "input_records": len(table_records),
                        "output_systems": len(systems),
                        "batches": 1,
                        "success": True,
                        "metadata": table_meta
                    })

                    logger.info(f"✅ {table_name}: {len(table_records)} records → {len(systems)} systems")

        except Exception as e:
            logger.error(f"❌ Failed to process table '{table_name}': {e}")
            table_results.append({
                "table_name": table_name,
                "output_systems": 0,
                "batches": 0,
                "success": False,
                "error": str(e)
            })

    # Add skipped tables to results
    for table_name, table_data in tables_skipped.items():
        if is_raw_docling:
            input_count = len(table_data.get('cells', []))
            count_key = "input_cells"
        else:
            input_count = len(table_data)
            count_key = "input_records"

        table_results.append({
            "table_name": table_name,
            count_key: input_count,
            "output_systems": 0,
            "batches": 0,
            "success": True,
            "skipped": True,
            "skip_reason": classifications[table_name]['reason']
        })

    logger.info("=== PDF Pipeline: Complete ===")
    logger.info(f"Processed {len(tables_to_process)} tables, extracted {len(all_systems)} systems")

    return {
        "systems": all_systems,
        "table_results": table_results,
        "stats": {
            "total_tables": num_tables,
            "processed_tables": len(tables_to_process),
            "skipped_tables": len(tables_skipped),
            "total_systems": len(all_systems),
            "format": "raw_docling" if is_raw_docling else "flattened"
        }
    }
