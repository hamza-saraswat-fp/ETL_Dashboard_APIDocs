"""
PDF Extractor - Stage 1 of ETL Pipeline
Simple extraction: PDF → Clean JSON (Flat Format)

Philosophy:
- Extract EVERYTHING (no filtering, no validation)
- Parse tables using Docling with optimal configuration
- Output flat JSON array (same format as Excel extractor)
- Let Stage 2 (LLM) handle understanding and filtering
"""

import json
from pathlib import Path
from typing import Optional, List
import logging

from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.pipeline_options import PdfPipelineOptions, TableFormerMode
from docling.datamodel.base_models import InputFormat

from .exceptions import (
    ExtractionError,
    FileNotFoundError as ExtractorFileNotFoundError,
    InvalidPDFFormatError,
    PDFProcessingError
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PDFExtractor:
    """
    Extracts tables from PDF files and outputs clean, machine-readable JSON

    Design: Dead simple. No intelligence, just extraction using Docling.
    Output format matches Excel extractor for unified Stage 2 processing.
    """

    def __init__(self, file_path: str):
        """
        Initialize extractor with PDF file

        Args:
            file_path: Path to the PDF file to extract

        Raises:
            ExtractorFileNotFoundError: If PDF file doesn't exist
            InvalidPDFFormatError: If file is not a valid PDF format
        """
        self.file_path = Path(file_path)

        if not self.file_path.exists():
            raise ExtractorFileNotFoundError(f"PDF file not found: {file_path}")

        if not self.file_path.suffix.lower() == '.pdf':
            raise InvalidPDFFormatError(
                f"File must be PDF format (.pdf): {file_path}"
            )

        logger.info(f"Initialized PDFExtractor for: {self.file_path.name}")

        # Configure pipeline for optimal table extraction
        try:
            # Pipeline options optimized for table extraction
            pipeline_options = PdfPipelineOptions(
                do_table_structure=True  # Enable table structure recognition
            )
            # Use ACCURATE mode for better table detection
            pipeline_options.table_structure_options.mode = TableFormerMode.ACCURATE
            # Enable cell matching for proper structure
            pipeline_options.table_structure_options.do_cell_matching = True

            # Initialize converter with table-optimized settings
            self.converter = DocumentConverter(
                format_options={
                    InputFormat.PDF: PdfFormatOption(
                        pipeline_options=pipeline_options
                    )
                }
            )
            logger.info("Docling DocumentConverter initialized with ACCURATE table mode")
        except Exception as e:
            raise ExtractionError(f"Failed to initialize Docling converter: {str(e)}")

    def extract_tables(self) -> List[dict]:
        """
        Extract all tables from PDF with rich cell structure

        Returns:
            List of table dictionaries with cell metadata (positions, spans, headers)

        Raises:
            PDFProcessingError: If PDF cannot be processed
        """
        logger.info(f"Processing PDF: {self.file_path.name}")

        try:
            # Convert PDF using Docling
            result = self.converter.convert(str(self.file_path))

            # Count tables found
            num_tables = len(result.document.tables)
            logger.info(f"Found {num_tables} tables in PDF")

            if num_tables == 0:
                logger.warning(f"No tables found in PDF: {self.file_path.name}")
                return []

            # Extract each table with rich cell structure
            all_tables = []
            for table_ix, table in enumerate(result.document.tables):
                try:
                    # Access table cell data directly
                    if not hasattr(table, 'data') or not hasattr(table.data, 'table_cells'):
                        logger.warning(f"Table {table_ix} has no cell data, skipping")
                        continue

                    cells = table.data.table_cells

                    if not cells or len(cells) == 0:
                        logger.warning(f"Table {table_ix} is empty, skipping")
                        continue

                    # Build table structure with cells
                    table_data = {
                        'table_id': table_ix,
                        'cells': []
                    }

                    # Extract each cell with metadata
                    for cell in cells:
                        cell_data = {
                            'text': cell.text if hasattr(cell, 'text') else '',
                            'row': cell.start_row_offset_idx if hasattr(cell, 'start_row_offset_idx') else 0,
                            'col': cell.start_col_offset_idx if hasattr(cell, 'start_col_offset_idx') else 0,
                            'row_span': cell.row_span if hasattr(cell, 'row_span') else 1,
                            'col_span': cell.col_span if hasattr(cell, 'col_span') else 1,
                            'is_column_header': cell.column_header if hasattr(cell, 'column_header') else False,
                            'is_row_header': cell.row_header if hasattr(cell, 'row_header') else False
                        }
                        table_data['cells'].append(cell_data)

                    num_cells = len(table_data['cells'])
                    all_tables.append(table_data)
                    logger.info(f"Extracted table {table_ix}: {num_cells} cells")

                except Exception as e:
                    logger.error(f"Error extracting table {table_ix}: {str(e)}")
                    continue

            logger.info(f"Successfully extracted {len(all_tables)} tables")
            return all_tables

        except Exception as e:
            raise PDFProcessingError(
                f"Failed to process PDF '{self.file_path.name}': {str(e)}"
            )

    def extract_to_json(self, output_dir: Optional[str] = None) -> str:
        """
        Extract all tables and save as rich JSON with cell structure

        Design: JSON object with tables array. Each table contains cells with
        positions, spans, headers. Bronze layer preserves all PDF structure.

        Args:
            output_dir: Directory to save JSON file (defaults to project_root/data/bronze/)

        Returns:
            Path to the output JSON file
        """
        # Find project root (go up from src/stage1_extractor to project root)
        if output_dir is None:
            project_root = Path(__file__).parent.parent.parent
            output_path = project_root / 'data' / 'bronze'
        else:
            output_path = Path(output_dir)

        output_path.mkdir(parents=True, exist_ok=True)

        base_filename = self.file_path.stem  # filename without extension

        logger.info(f"=== Starting extraction ===")

        # Extract tables with rich structure
        try:
            all_tables = self.extract_tables()
        except PDFProcessingError as e:
            logger.error(f"Error processing PDF: {e}")
            raise

        # Build output JSON structure
        output_data = {
            'source_file': self.file_path.name,
            'source_type': 'pdf',
            'tables': all_tables
        }

        # Save JSON with rich structure
        output_filename = f"{base_filename}.json"
        output_filepath = output_path / output_filename

        with open(output_filepath, 'w') as f:
            json.dump(output_data, f, indent=2)

        # Calculate stats
        total_cells = sum(len(table['cells']) for table in all_tables)

        logger.info(f"=== Extraction complete ===")
        logger.info(f"Output: {output_filepath}")
        logger.info(f"Total tables: {len(all_tables)}")
        logger.info(f"Total cells: {total_cells:,}")

        return str(output_filepath)


def main():
    """
    Example usage of PDFExtractor
    """
    import sys

    if len(sys.argv) < 2:
        print("Usage: python pdf_extractor.py <path_to_pdf_file>")
        print("Example: python pdf_extractor.py ../../data/raw/catalog_2025.pdf")
        sys.exit(1)

    file_path = sys.argv[1]

    try:
        extractor = PDFExtractor(file_path)
        output_path = extractor.extract_to_json()

        print("\n=== Extraction Complete ===")
        print(f"Output JSON: {output_path}")
        print("\nReady for Stage 2 (LLM Transformer)")

    except ExtractorFileNotFoundError as e:
        print(f"\nError: {e}")
        print("Please check the file path and try again.")
        sys.exit(1)

    except InvalidPDFFormatError as e:
        print(f"\nError: {e}")
        print("Supported format: .pdf")
        sys.exit(1)

    except PDFProcessingError as e:
        print(f"\nError: {e}")
        print("The PDF file could not be processed.")
        sys.exit(1)

    except ExtractionError as e:
        print(f"\nExtraction Error: {e}")
        sys.exit(1)

    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        print(f"\nUnexpected error occurred. Check logs for details.")
        sys.exit(1)


if __name__ == "__main__":
    main()
