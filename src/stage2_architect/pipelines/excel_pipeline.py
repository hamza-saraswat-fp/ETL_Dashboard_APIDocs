"""
Excel Pipeline - Orchestrates Excel bronze to silver transformation

Flow:
1. Load bronze JSON
2. Batch by source_sheet
3. Classify sheets (filter non-system sheets)
4. Transform each sheet using LLM (via architect)
5. Combine and return results
"""

import logging
from typing import List, Dict, Any

from ..batchers import batch_by_sheet, get_sheet_stats, batch_large_sheet
from ..classifiers import classify_sheets

logger = logging.getLogger(__name__)


def process_excel_bronze(
    bronze_data: List[dict],
    llm_transform_fn,
    prompt_template: str
) -> Dict[str, Any]:
    """
    Process Excel bronze data through classification, batching, and LLM transformation

    Args:
        bronze_data: List of bronze records with source_sheet field
        llm_transform_fn: Function to call for LLM transformation
            Signature: (sheet_name: str, records: List[dict]) -> tuple[list, dict]
        prompt_template: Base prompt template for LLM

    Returns:
        Dictionary with:
        - systems: List of all extracted systems
        - sheet_results: List of per-sheet processing results
        - stats: Processing statistics
    """
    logger.info("=== Excel Pipeline: Starting ===")

    # Step 1: Batch by sheet
    logger.info("Step 1: Batching records by source_sheet")
    sheets = batch_by_sheet(bronze_data)
    sheet_stats = get_sheet_stats(sheets)
    logger.info(f"Found {sheet_stats['total_sheets']} sheets with {sheet_stats['total_records']} total records")

    # Step 2: Classify sheets (filter out non-system sheets)
    logger.info(f"Step 2: Classifying {sheet_stats['total_sheets']} sheets")
    classifications = classify_sheets(sheets)

    # Separate processable and skipped sheets
    sheets_to_process = {name: records for name, records in sheets.items()
                        if not classifications[name]['skip']}
    sheets_skipped = {name: records for name, records in sheets.items()
                     if classifications[name]['skip']}

    logger.info(f"Processing {len(sheets_to_process)} sheets, skipping {len(sheets_skipped)} sheets")

    # Step 3: Transform each sheet using LLM (with batching for large sheets)
    logger.info(f"Step 3: Transforming {len(sheets_to_process)} sheets using LLM")
    all_systems = []
    sheet_results = []

    for sheet_name, sheet_records in sheets_to_process.items():
        logger.info(f"\n--- Processing sheet: {sheet_name} ---")
        logger.info(f"Records: {len(sheet_records)}")

        try:
            # Check if sheet needs batching (>30 records)
            if len(sheet_records) > 30:
                logger.info(f"Large sheet detected, splitting into batches...")
                record_batches = batch_large_sheet(sheet_records, batch_size=30)

                sheet_systems = []
                for batch_idx, batch_records in enumerate(record_batches, 1):
                    logger.info(f"  Processing batch {batch_idx}/{len(record_batches)} ({len(batch_records)} records)")

                    batch_systems, batch_meta = llm_transform_fn(
                        f"{sheet_name} (batch {batch_idx})",
                        batch_records
                    )
                    sheet_systems.extend(batch_systems)
                    logger.info(f"  ✅ Batch {batch_idx}: {len(batch_records)} records → {len(batch_systems)} systems")

                all_systems.extend(sheet_systems)
                sheet_results.append({
                    "sheet_name": sheet_name,
                    "input_records": len(sheet_records),
                    "output_systems": len(sheet_systems),
                    "batches": len(record_batches),
                    "success": True
                })

                logger.info(f"✅ {sheet_name}: {len(sheet_records)} records → {len(sheet_systems)} systems (via {len(record_batches)} batches)")

            else:
                # Process entire sheet in one call
                systems, sheet_meta = llm_transform_fn(sheet_name, sheet_records)

                all_systems.extend(systems)
                sheet_results.append({
                    "sheet_name": sheet_name,
                    "input_records": len(sheet_records),
                    "output_systems": len(systems),
                    "batches": 1,
                    "success": True,
                    "metadata": sheet_meta
                })

                logger.info(f"✅ {sheet_name}: {len(sheet_records)} records → {len(systems)} systems")

        except Exception as e:
            logger.error(f"❌ Failed to process sheet '{sheet_name}': {e}")
            sheet_results.append({
                "sheet_name": sheet_name,
                "input_records": len(sheet_records),
                "output_systems": 0,
                "batches": 0,
                "success": False,
                "error": str(e)
            })

    # Add skipped sheets to results
    for sheet_name, records in sheets_skipped.items():
        sheet_results.append({
            "sheet_name": sheet_name,
            "input_records": len(records),
            "output_systems": 0,
            "batches": 0,
            "success": True,
            "skipped": True,
            "skip_reason": classifications[sheet_name]['reason']
        })

    logger.info("=== Excel Pipeline: Complete ===")
    logger.info(f"Processed {len(sheets_to_process)} sheets, extracted {len(all_systems)} systems")

    return {
        "systems": all_systems,
        "sheet_results": sheet_results,
        "stats": {
            "total_sheets": sheet_stats['total_sheets'],
            "processed_sheets": len(sheets_to_process),
            "skipped_sheets": len(sheets_skipped),
            "total_systems": len(all_systems)
        }
    }
