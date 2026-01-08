"""
Module 2: Bronze to Silver JSON Transformer
Transforms bronze layer JSON to silver layer using LLM
"""

import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional

from .llm_client import LLMClient
from .silver_validator import validate_silver
from .pipelines import process_excel_bronze, process_pdf_bronze

# Set up logging (will be configured in main() based on verbose flag)
logger = logging.getLogger(__name__)


def detect_source_type(bronze_data) -> str:
    """
    Detect whether bronze data is from Excel or PDF source

    Args:
        bronze_data: Either:
            - Dict with {source_file, source_type, tables} (raw Docling PDF)
            - List of bronze records (Excel or flattened)

    Returns:
        'excel' if source_sheet field exists, 'pdf' if source_type is pdf or source_table exists

    Raises:
        ValueError: If source type cannot be determined
    """
    # Handle raw Docling PDF format (dict)
    if isinstance(bronze_data, dict):
        if 'source_type' in bronze_data and bronze_data['source_type'] == 'pdf':
            return 'pdf'
        else:
            raise ValueError(
                f"Cannot determine source type from dict. Expected 'source_type': 'pdf', "
                f"got: {bronze_data.get('source_type')}"
            )

    # Handle list format (Excel or flattened PDF)
    if not bronze_data or len(bronze_data) == 0:
        raise ValueError("Bronze data is empty")

    # Check first record for source type
    first_record = bronze_data[0]

    if 'source_sheet' in first_record:
        return 'excel'
    elif 'source_table' in first_record:
        return 'pdf'
    else:
        raise ValueError(
            "Cannot determine source type: bronze data must have either "
            "'source_sheet' (Excel) or 'source_table' (PDF) field"
        )


class BronzeJSONTransformer:
    """Transforms bronze layer JSON to silver layer JSON using LLM"""

    def __init__(
        self,
        api_key: str,
        model: str = "anthropic/claude-sonnet-4.5",
        enable_ahri_enrichment: bool = False,
        job_id: Optional[str] = None,
        job_logger: Optional[Any] = None
    ):
        """
        Initialize transformer

        Args:
            api_key: OpenRouter API key
            model: LLM model identifier (default: Claude Sonnet 4.5)
            enable_ahri_enrichment: Enable AHRI enrichment for missing data
            job_id: Optional job ID for lineage tracking
            job_logger: Optional JobLogger for structured logging
        """
        self.api_key = api_key
        self.llm_client = LLMClient(api_key, model=model)
        self.enable_ahri_enrichment = enable_ahri_enrichment
        self.job_id = job_id
        self.job_logger = job_logger

        logger.info(f"Initialized BronzeJSONTransformer with model: {model}")
        if enable_ahri_enrichment:
            logger.info("AHRI enrichment: ENABLED")

    def _load_prompt_template(self, source_type: str) -> str:
        """
        Load transformation prompt from prompts directory based on source type

        Args:
            source_type: 'excel' or 'pdf'

        Returns:
            Prompt template string
        """
        project_root = Path(__file__).parent.parent.parent
        prompt_path = project_root / "config" / "prompts" / source_type / "bronze_to_silver_transform.md"

        if not prompt_path.exists():
            raise FileNotFoundError(f"Prompt template not found: {prompt_path}")

        with open(prompt_path, 'r') as f:
            prompt = f.read()

        logger.info(f"Loaded prompt template: {prompt_path}")
        logger.info(f"Prompt size: {len(prompt)} characters")

        return prompt

    def transform(self, bronze_json_path: str, output_dir: str = None) -> Dict[str, Any]:
        """
        Transform bronze JSON to silver JSON

        Args:
            bronze_json_path: Path to bronze layer JSON file
            output_dir: Optional output directory (defaults to data/silver/)

        Returns:
            Dictionary with transformation results:
            - silver_path: path to output file
            - sources_processed: number of sheets/tables processed
            - total_systems: number of systems extracted
            - validation: validation results
            - metadata: detailed processing metadata
        """
        logger.info(f"=== Starting Bronze → Silver Transformation ===")
        logger.info(f"Input: {bronze_json_path}")

        # Step 1: Load bronze JSON
        logger.info("Step 1: Loading bronze JSON")
        bronze_data = self._load_bronze_json(bronze_json_path)

        # Log data format info
        is_dict = isinstance(bronze_data, dict)
        if is_dict:
            logger.info(f"Loaded dict format with {len(bronze_data.get('tables', []))} tables")
        else:
            logger.info(f"Loaded {len(bronze_data)} records")

        # Step 2: Detect source type (Excel vs PDF)
        logger.info("Step 2: Detecting source type")
        source_type = detect_source_type(bronze_data)
        logger.info(f"Detected source type: {source_type.upper()}")

        # Step 2.5: Load appropriate prompt template
        logger.info(f"Step 2.5: Loading {source_type} prompt template")
        self.prompt_template = self._load_prompt_template(source_type)

        # Step 2.6: Clean PDF data (remove null columns to reduce LLM costs)
        # Only for legacy flattened PDF format (list), skip for raw Docling (dict)
        if source_type == 'pdf' and isinstance(bronze_data, list):
            logger.info("Step 2.6: Cleaning flattened PDF data (removing null columns)")
            bronze_data = self._clean_null_columns(bronze_data)

        # Step 3: Route to appropriate pipeline
        logger.info(f"Step 3: Routing to {source_type} pipeline")

        if source_type == 'excel':
            pipeline_result = process_excel_bronze(
                bronze_data,
                llm_transform_fn=self._transform_source,
                prompt_template=self.prompt_template
            )
            source_results_key = 'sheet_results'
        elif source_type == 'pdf':
            # Unified PDF pipeline: handles both raw Docling and legacy flattened
            pipeline_result = process_pdf_bronze(
                bronze_data,
                llm_transform_fn=self._transform_source,
                prompt_template=self.prompt_template
            )
            source_results_key = 'table_results'
        else:
            raise ValueError(f"Unknown source type: {source_type}")

        all_systems = pipeline_result['systems']
        source_results = pipeline_result[source_results_key]

        # Step 4: Validate results
        logger.info(f"\nStep 4: Combining results and validating")
        silver_data = {"systems": all_systems}

        validation_result = validate_silver(silver_data)

        if validation_result["valid"]:
            logger.info(f"✅ Validation passed")
        else:
            logger.warning(f"⚠️  Validation found {len(validation_result['errors'])} errors")

        logger.info(f"Validation stats: {validation_result['stats']}")

        # Step 4.5: AHRI Enrichment (optional)
        if self.enable_ahri_enrichment:
            logger.info(f"\nStep 4.5: AHRI Enrichment")
            try:
                from .ahri_enrichment import AHRIEnricher

                enricher = AHRIEnricher()
                systems_before = len(all_systems)

                # Check how many need enrichment
                from .ahri_enrichment.validator import needs_enrichment
                systems_needing = sum(1 for s in all_systems if needs_enrichment(s))
                logger.info(f"Systems needing enrichment: {systems_needing}/{systems_before}")

                if systems_needing > 0:
                    all_systems = enricher.enrich_systems(all_systems)
                    silver_data = {"systems": all_systems}

                    # Re-validate after enrichment
                    validation_result = validate_silver(silver_data)
                    logger.info(f"✅ AHRI enrichment complete")
                else:
                    logger.info("No systems need enrichment, skipping")

            except Exception as e:
                logger.error(f"AHRI enrichment failed (non-fatal): {e}")
                logger.info("Continuing with un-enriched data")

        # Step 5: Save output
        logger.info("Step 5: Saving silver layer JSON")
        silver_path = self._save_silver_json(bronze_json_path, silver_data, output_dir)

        # Summary
        logger.info(f"\n=== Transformation Complete ===")
        logger.info(f"Output: {silver_path}")
        logger.info(f"Source type: {source_type.upper()}")
        logger.info(f"Sources processed: {len(source_results)}")
        logger.info(f"Total systems: {len(all_systems)}")
        logger.info(f"Validation: {'✅ PASSED' if validation_result['valid'] else '⚠️  WARNINGS'}")

        result = {
            "silver_path": silver_path,
            "source_type": source_type,
            "sources_processed": len(source_results),
            "total_systems": len(all_systems),
            "validation": validation_result,
            "metadata": {
                "input_file": bronze_json_path,
                "input_records": len(bronze_data),
                "source_results": source_results,
                "transformation_date": datetime.now().isoformat()
            }
        }

        return result

    def _load_bronze_json(self, bronze_json_path: str):
        """
        Load bronze layer JSON file

        Returns:
            For PDF (raw Docling): dict with {source_file, source_type, tables}
            For Excel: list of records
        """
        path = Path(bronze_json_path)

        if not path.exists():
            raise FileNotFoundError(f"Bronze JSON file not found: {bronze_json_path}")

        with open(path, 'r') as f:
            data = json.load(f)

        # Handle both formats:
        # - Raw Docling PDF: dict with {source_file, source_type, tables}
        # - Excel/flattened: list of records
        if isinstance(data, dict):
            # Raw Docling PDF format - return as-is
            if 'source_type' in data and data['source_type'] == 'pdf':
                return data
            else:
                raise ValueError(f"Bronze JSON dict must have 'source_type': 'pdf', got {data.get('source_type')}")
        elif isinstance(data, list):
            # Excel or flattened format - return as-is
            return data
        else:
            raise ValueError(f"Bronze JSON must be a dict or array, got {type(data)}")

        return data

    def _clean_null_columns(self, bronze_data: List[dict], min_data_threshold: float = 0.05) -> List[dict]:
        """
        Remove sparse columns with insufficient data (PDF optimization)

        This dramatically reduces token count for LLM calls by removing
        sparse columns that provide no value but consume tokens.

        Args:
            bronze_data: List of bronze records
            min_data_threshold: Minimum fraction of records that must have non-null data
                              to keep the column (default: 0.05 = 5%)

        Returns:
            Cleaned data with sparse columns removed
        """
        if not bronze_data or len(bronze_data) == 0:
            return bronze_data

        total_records = len(bronze_data)

        # Count non-null values for each column
        column_counts = {}

        for record in bronze_data:
            for key, value in record.items():
                if key not in column_counts:
                    column_counts[key] = 0

                # Check if value is meaningful (not null/empty)
                if value not in [None, '', 'nan', 'N/A', 'n/a', 'null', 'None']:
                    # Also check it's not just whitespace
                    if isinstance(value, str) and value.strip() == '':
                        continue
                    column_counts[key] += 1

        # Determine which columns to keep based on threshold
        columns_to_keep = set()

        for col, count in column_counts.items():
            data_ratio = count / total_records

            # Always keep source_table or source_sheet
            if col in ['source_table', 'source_sheet']:
                columns_to_keep.add(col)
            # Keep if column meets minimum data threshold
            elif data_ratio >= min_data_threshold:
                columns_to_keep.add(col)

        # Create cleaned records with only columns that meet threshold
        cleaned_data = []
        for record in bronze_data:
            cleaned_record = {
                key: value for key, value in record.items()
                if key in columns_to_keep
            }
            cleaned_data.append(cleaned_record)

        # Log the optimization
        original_cols = len(column_counts)
        cleaned_cols = len(columns_to_keep)
        removed = original_cols - cleaned_cols
        threshold_pct = int(min_data_threshold * 100)

        logger.info(f"Cleaned sparse columns ({threshold_pct}% threshold): {original_cols} → {cleaned_cols} columns ({removed} removed, {removed/original_cols*100:.1f}% reduction)")

        return cleaned_data

    def _transform_source(self, source_name: str, source_data) -> tuple:
        """
        Transform a single source (sheet/table) using LLM

        Unified method: handles both flat records and cell-based structures.

        Args:
            source_name: Name of the source (sheet or table)
            source_data: Either:
                - List of bronze records: [{...}, {...}] (Excel/legacy PDF)
                - Dict with cells: {"table_id": 0, "cells": [...]} (raw Docling PDF)

        Returns:
            Tuple of (systems_list, metadata_dict)
        """
        # Detect data type and build appropriate prompt
        is_cell_based = isinstance(source_data, dict) and 'cells' in source_data

        if is_cell_based:
            prompt = self._build_prompt_for_cells(source_name, source_data)
            input_count = len(source_data.get('cells', []))
            input_type = "cells"
        else:
            prompt = self._build_prompt(source_name, source_data)
            input_count = len(source_data)
            input_type = "records"

        # Call LLM with increased max_tokens for larger outputs
        start_time = datetime.now()
        response = self.llm_client.transform_data(
            prompt,
            max_tokens=25000,
            job_logger=self.job_logger
        )
        end_time = datetime.now()

        processing_time = (end_time - start_time).total_seconds()

        # Parse response
        silver_data = json.loads(response)

        if "systems" not in silver_data:
            raise ValueError(f"LLM response missing 'systems' key")

        systems = silver_data["systems"]

        metadata = {
            "source_name": source_name,
            f"input_{input_type}": input_count,
            "output_systems": len(systems),
            "processing_time_seconds": round(processing_time, 2),
            "format": "cell_based" if is_cell_based else "flat_records"
        }

        return systems, metadata

    def _build_prompt(self, source_name: str, records: List[dict]) -> str:
        """
        Build complete prompt for LLM (flat records format)

        Args:
            source_name: Name of the source (sheet/table) being processed
            records: Bronze records for this source

        Returns:
            Complete prompt string
        """
        # Add context about the source
        source_context = f"""
## SOURCE CONTEXT

You are processing source: **{source_name}**
Total records in this batch: {len(records)}
"""

        # Add the input data (compact JSON to reduce token count)
        input_data = f"""
## INPUT DATA (Bronze Layer JSON)

{json.dumps(records)}
"""

        # Add instruction
        instruction = """

Transform the above bronze layer data into silver layer format following the schema and guidelines provided above.
Remember to output ONLY the JSON object (starting with {{ and ending with }}).
"""

        # Combine: base prompt + source context + input data + instruction
        full_prompt = self.prompt_template + source_context + input_data + instruction

        return full_prompt

    def _build_prompt_for_cells(self, table_name: str, table_data: dict) -> str:
        """
        Build complete prompt for LLM (cell-based format for raw Docling)

        Args:
            table_name: Name of the table being processed
            table_data: Dict with cells: {"table_id": 0, "cells": [...]}

        Returns:
            Complete prompt string
        """
        num_cells = len(table_data.get('cells', []))

        # Add context about the source
        source_context = f"""
## SOURCE CONTEXT

You are processing PDF table: **{table_name}**
Total cells in this table: {num_cells}

This data is from a PDF table extracted using Docling. Each cell has:
- text: The cell content
- row, col: Position in the table (0-indexed)
- row_span, col_span: How many rows/columns the cell spans
- is_column_header: True if this is a column header
- is_row_header: True if this is a row header

Use the cell positions and header flags to understand the table structure.
"""

        # Add the input data (compact JSON to reduce token count)
        input_data = f"""
## INPUT DATA (Bronze Layer JSON - Docling Cell Format)

{json.dumps(table_data)}
"""

        # Add instruction
        instruction = """

Transform the above bronze layer data into silver layer format following the schema and guidelines provided above.
Use the cell positions (row, col) and header flags to reconstruct the table structure.
Remember to output ONLY the JSON object (starting with {{ and ending with }}).
"""

        # Combine: base prompt + source context + input data + instruction
        full_prompt = self.prompt_template + source_context + input_data + instruction

        return full_prompt

    def _save_silver_json(self, bronze_path: str, silver_data: dict, output_dir: str = None) -> str:
        """Save silver layer JSON"""
        # Determine output directory
        if output_dir is None:
            project_root = Path(__file__).parent.parent.parent
            output_path = project_root / "data" / "silver"
        else:
            output_path = Path(output_dir)

        output_path.mkdir(parents=True, exist_ok=True)

        # Generate filename from bronze filename
        bronze_filename = Path(bronze_path).stem
        silver_filename = f"{bronze_filename}.json"
        silver_filepath = output_path / silver_filename

        # Save with pretty formatting
        with open(silver_filepath, 'w') as f:
            json.dump(silver_data, f, indent=2)

        logger.info(f"Saved silver JSON: {silver_filepath}")
        logger.info(f"File size: {silver_filepath.stat().st_size / 1024:.1f} KB")

        return str(silver_filepath)


def main():
    """CLI entry point for Module 2 transformer"""
    import sys
    import os

    # Try to load API key from .env file
    env_path = Path(__file__).parent.parent.parent / '.env'
    api_key = None

    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                if line.startswith('OPENROUTER_API_KEY='):
                    api_key = line.strip().split('=', 1)[1]
                    break

    # Check command line arguments
    if len(sys.argv) < 2:
        print("Usage: python -m src.stage2_architect.bronze_json_transformer <bronze_json_path> [api_key] [model] [--enable-ahri-enrichment] [--verbose]")
        print("Example: python -m src.stage2_architect.bronze_json_transformer data/bronze/GE_NDP_UPDATE_09_2025.json")
        print("Example with enrichment: python -m src.stage2_architect.bronze_json_transformer data/bronze/GE_NDP_UPDATE_09_2025.json --enable-ahri-enrichment")
        print("Example with verbose logging: python -m src.stage2_architect.bronze_json_transformer data/bronze/GE_NDP_UPDATE_09_2025.json --verbose")
        print("\nAPI key can be provided via:")
        print("  1. Command line argument")
        print("  2. .env file (OPENROUTER_API_KEY=...)")
        print("\nModel (optional):")
        print("  Default: anthropic/claude-sonnet-4.5")
        print("  Alternative: anthropic/claude-sonnet-4-20250514")
        print("\nOptions:")
        print("  --enable-ahri-enrichment    Enable AHRI enrichment for systems with missing data")
        print("  --verbose                   Enable verbose (DEBUG) logging with detailed API info")
        sys.exit(1)

    bronze_json_path = sys.argv[1]

    # Check for flags (can be anywhere in args)
    enable_ahri_enrichment = '--enable-ahri-enrichment' in sys.argv
    verbose = '--verbose' in sys.argv

    # Set up logging based on verbose flag
    log_level = logging.DEBUG if verbose else logging.INFO
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # Create logs directory
    project_root = Path(__file__).parent.parent.parent
    logs_dir = project_root / "logs"
    logs_dir.mkdir(exist_ok=True)

    # Generate log filename with timestamp
    bronze_filename = Path(bronze_json_path).stem
    log_filename = f"stage2_{bronze_filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_filepath = logs_dir / log_filename

    # Configure logging to both file and console
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.FileHandler(log_filepath),
            logging.StreamHandler(sys.stdout)
        ]
    )

    logger.info(f"{'='*60}")
    logger.info(f"Stage 2: Bronze → Silver Transformation")
    logger.info(f"Log level: {'DEBUG (verbose)' if verbose else 'INFO'}")
    logger.info(f"Log file: {log_filepath}")
    logger.info(f"{'='*60}")

    # Override with command line API key if provided (skip if it's a flag)
    if len(sys.argv) >= 3 and not sys.argv[2].startswith('--'):
        api_key = sys.argv[2]

    if not api_key:
        print("Error: No API key found. Please provide via command line or .env file.")
        sys.exit(1)

    # Optional model parameter (skip if it's a flag)
    model = "anthropic/claude-sonnet-4.5"
    if len(sys.argv) >= 4 and not sys.argv[3].startswith('--'):
        model = sys.argv[3]

    try:
        transformer = BronzeJSONTransformer(api_key, model=model, enable_ahri_enrichment=enable_ahri_enrichment)
        result = transformer.transform(bronze_json_path)

        print("\n=== Transformation Complete ===")
        print(f"Silver JSON: {result['silver_path']}")
        print(f"Sources processed: {result['sources_processed']}")
        print(f"Total systems extracted: {result['total_systems']}")
        print(f"Validation: {'✅ PASSED' if result['validation']['valid'] else '⚠️  HAS WARNINGS'}")

        if not result['validation']['valid']:
            print(f"\nValidation errors: {len(result['validation']['errors'])}")
            for error in result['validation']['errors'][:5]:
                print(f"  - {error}")
            if len(result['validation']['errors']) > 5:
                print(f"  ... and {len(result['validation']['errors']) - 5} more")

        print(f"\nStats:")
        for key, value in result['validation']['stats'].items():
            print(f"  {key}: {value}")

    except FileNotFoundError as e:
        print(f"\nError: {e}")
        print("Please check the file path and try again.")
        sys.exit(1)

    except Exception as e:
        logger.error(f"Error during transformation: {e}", exc_info=True)
        print(f"\nError: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
