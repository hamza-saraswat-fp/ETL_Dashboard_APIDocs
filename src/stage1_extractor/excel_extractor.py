"""
Excel Extractor - Stage 1 of ETL Pipeline
Simple extraction: Excel → Clean JSON

Philosophy:
- Extract EVERYTHING (no filtering, no validation)
- Find headers, clean data, preserve integrity
- Output JSON array with source_sheet field
- Let Stage 2 (LLM) handle understanding and filtering
"""

import pandas as pd
from pathlib import Path
from typing import Optional, List, Tuple
import logging

from .config import (
    HEADER_KEYWORDS,
    MIN_KEYWORD_MATCHES,
    MAX_HEADER_SCAN_ROWS,
    SECTION_DELIMITER,
    MIN_SECTION_GAP
)
from .exceptions import (
    ExtractionError,
    FileNotFoundError as ExtractorFileNotFoundError,
    InvalidFileFormatError,
    SheetProcessingError,
    NoValidSheetsError
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ExcelExtractor:
    """
    Extracts data from Excel files and outputs clean, machine-readable JSON

    Design: Dead simple. No intelligence, just extraction.
    """

    def __init__(self, file_path: str):
        """
        Initialize extractor with Excel file

        Args:
            file_path: Path to the Excel file to extract

        Raises:
            ExtractorFileNotFoundError: If Excel file doesn't exist
            InvalidFileFormatError: If file is not a valid Excel format
        """
        self.file_path = Path(file_path)

        if not self.file_path.exists():
            raise ExtractorFileNotFoundError(f"Excel file not found: {file_path}")

        if not self.file_path.suffix.lower() in ['.xlsx', '.xls', '.xlsm', '.xlsb']:
            raise InvalidFileFormatError(
                f"File must be Excel format (.xlsx, .xls, .xlsm, .xlsb): {file_path}"
            )

        logger.info(f"Initialized ExcelExtractor for: {self.file_path.name}")

        try:
            # Detect if file is xlsb format
            self.is_xlsb = self.file_path.suffix.lower() == '.xlsb'

            # Load all sheets (use pyxlsb engine for xlsb files)
            if self.is_xlsb:
                self.excel_file = pd.ExcelFile(self.file_path, engine='pyxlsb')
            else:
                self.excel_file = pd.ExcelFile(self.file_path)

            all_sheet_names = self.excel_file.sheet_names

            # Filter out hidden sheets (only for non-xlsb, pyxlsb doesn't support sheet visibility)
            if self.is_xlsb:
                # xlsb: process all sheets (pyxlsb doesn't expose sheet visibility)
                self.sheet_names = all_sheet_names
                logger.info(f"Found {len(all_sheet_names)} sheets (xlsb format - processing all)")
            else:
                # xlsx/xls/xlsm: filter hidden sheets using openpyxl
                visible_sheets = []
                hidden_sheets = []

                for sheet_name in all_sheet_names:
                    sheet = self.excel_file.book[sheet_name]
                    if sheet.sheet_state == 'visible':
                        visible_sheets.append(sheet_name)
                    else:
                        hidden_sheets.append(sheet_name)

                self.sheet_names = visible_sheets

                logger.info(f"Found {len(all_sheet_names)} total sheets")
                if hidden_sheets:
                    logger.info(f"Skipping {len(hidden_sheets)} hidden sheets: {hidden_sheets}")
                logger.info(f"Processing {len(self.sheet_names)} visible sheets: {self.sheet_names}")
        except Exception as e:
            raise ExtractionError(f"Failed to load Excel file: {str(e)}")

    def find_header_row(self, df: pd.DataFrame, keywords: List[str] = HEADER_KEYWORDS) -> Optional[int]:
        """
        Scan dataframe to find the header row containing column keywords

        Args:
            df: DataFrame to scan
            keywords: List of keywords to search for in header row

        Returns:
            Row index of header, or None if not found
        """
        # Convert keywords to lowercase for case-insensitive matching
        keywords_lower = [k.lower() for k in keywords]

        # Scan first N rows
        rows_to_scan = min(MAX_HEADER_SCAN_ROWS, len(df))

        for idx in range(rows_to_scan):
            # Get all values in this row as strings (lowercase)
            row_values = df.iloc[idx].astype(str).str.lower().tolist()

            # Count how many keywords are present in this row
            matches = sum(
                1 for val in row_values
                if any(keyword in str(val) for keyword in keywords_lower)
            )

            if matches >= MIN_KEYWORD_MATCHES:
                logger.info(f"Found header row at index {idx} with {matches} keyword matches")
                return idx

        logger.warning(f"No header row found in first {rows_to_scan} rows")
        return None

    def find_all_header_rows(self, df: pd.DataFrame) -> List[int]:
        """
        Find ALL header rows in the sheet, not just the first.

        Scans entire DataFrame for rows that look like headers based on
        keyword matching. Used to detect multi-section sheets where each
        section has its own header with potentially different column layouts.

        Args:
            df: DataFrame to scan

        Returns:
            List of row indices where headers are found. Returns [0] as fallback.
        """
        keywords_lower = [k.lower() for k in HEADER_KEYWORDS]
        header_rows = []

        for idx in range(len(df)):
            row_values = df.iloc[idx].astype(str).str.lower().tolist()
            matches = sum(
                1 for val in row_values
                if any(keyword in str(val) for keyword in keywords_lower)
            )

            if matches >= MIN_KEYWORD_MATCHES:
                # Require gap from previous header to avoid false positives
                # (data rows containing "model" or "price" text)
                if not header_rows or idx > header_rows[-1] + MIN_SECTION_GAP:
                    header_rows.append(idx)
                    logger.debug(f"Found header row at index {idx} with {matches} keyword matches")

        if not header_rows:
            logger.warning("No header rows found, using row 0 as fallback")
            return [0]

        logger.info(f"Found {len(header_rows)} header row(s) at indices: {header_rows}")
        return header_rows

    def _extract_section_name(self, df: pd.DataFrame, header_idx: int, section_num: int) -> str:
        """
        Extract section name from the row above the header.

        Looks for section titles like "GAS SYSTEMS" or "HEAT PUMP" in the row
        immediately preceding the header row. Falls back to SECTION_N if not found.

        Args:
            df: Original DataFrame (before cleaning)
            header_idx: Index of the header row
            section_num: Section number for fallback naming (1-indexed)

        Returns:
            Section name (uppercase, spaces replaced with underscores)
        """
        if header_idx > 0:
            row_above = df.iloc[header_idx - 1]
            for val in row_above:
                val_str = str(val).strip()
                # Look for non-empty cell that could be a section title
                # Must be longer than 2 chars and not just 'nan'
                if val_str and val_str.lower() != 'nan' and len(val_str) > 2:
                    # Clean and format as section name
                    section_name = val_str.upper().replace(' ', '_')
                    # Remove any characters that might cause issues
                    section_name = ''.join(c for c in section_name if c.isalnum() or c == '_')
                    if section_name:
                        return section_name

        return f"SECTION_{section_num}"

    def split_into_sections(self, df: pd.DataFrame) -> List[Tuple[str, pd.DataFrame]]:
        """
        Split DataFrame into sections based on header rows.

        Detects all header rows in the sheet and splits the data at each
        header boundary. Each section is processed independently with its
        own column structure.

        Args:
            df: Raw DataFrame (before cleaning)

        Returns:
            List of (section_name, cleaned_df) tuples.
            section_name is empty string for single-section sheets.
        """
        header_rows = self.find_all_header_rows(df)

        if len(header_rows) == 1:
            # Single section - use existing logic, no section suffix
            cleaned = self.clean_dataframe(df, header_rows[0])
            return [("", cleaned)]

        logger.info(f"Detected {len(header_rows)} sections in sheet")

        sections = []
        for i, header_idx in enumerate(header_rows):
            # Section ends at next header or end of DataFrame
            end_idx = header_rows[i + 1] if i + 1 < len(header_rows) else len(df)

            # Extract section slice
            section_df = df.iloc[header_idx:end_idx].copy().reset_index(drop=True)

            # Get section name from row above header
            section_name = self._extract_section_name(df, header_idx, i + 1)

            # Clean this section with its header at row 0 (relative to slice)
            cleaned = self.clean_dataframe(section_df, header_row_idx=0)

            if not cleaned.empty:
                sections.append((section_name, cleaned))
                logger.info(f"Section '{section_name}': {len(cleaned)} rows, {len(cleaned.columns)} columns")
            else:
                logger.warning(f"Section '{section_name}' is empty after cleaning, skipping")

        return sections

    def clean_dataframe(self, df: pd.DataFrame, header_row_idx: Optional[int] = None) -> pd.DataFrame:
        """
        Clean dataframe by setting proper header and removing garbage rows

        Args:
            df: DataFrame to clean
            header_row_idx: Index of header row (will auto-detect if None)

        Returns:
            Cleaned DataFrame
        """
        # Find header if not provided
        if header_row_idx is None:
            header_row_idx = self.find_header_row(df)

        if header_row_idx is None:
            logger.warning("No header found, using row 0 as header")
            header_row_idx = 0

        # Set the header row as column names
        df.columns = df.iloc[header_row_idx]

        # Ensure unique column names (handle duplicates and empty names)
        new_cols = []
        col_counts = {}
        for col in df.columns:
            col_str = str(col).strip()
            if col_str == '' or col_str == 'nan':
                col_str = 'Unnamed'

            if col_str in col_counts:
                col_counts[col_str] += 1
                new_cols.append(f"{col_str}_{col_counts[col_str]}")
            else:
                col_counts[col_str] = 0
                new_cols.append(col_str)

        df.columns = new_cols

        # Remove all rows before and including the header
        df = df.iloc[header_row_idx + 1:]

        # Remove completely empty rows
        df = df.dropna(how='all')

        # Reset index
        df = df.reset_index(drop=True)

        # Remove rows where all values are empty strings
        df = df[~df.apply(lambda row: all(str(val).strip() == '' for val in row), axis=1)]

        # Reset index again after filtering
        df = df.reset_index(drop=True)

        logger.info(f"Cleaned dataframe: {len(df)} rows, {len(df.columns)} columns")

        return df

    def process_sheet(self, sheet_name: str) -> List[Tuple[str, pd.DataFrame]]:
        """
        Process a single sheet from the Excel file, detecting multiple sections.

        Design principles:
        - Read all data as strings to preserve data integrity
        - Prevents Excel auto-conversion issues (dates, leading zeros, etc.)
        - Replace NaN with empty strings for consistent handling
        - Detect multiple sections with different column layouts

        Args:
            sheet_name: Name of the sheet to process

        Returns:
            List of (section_name, cleaned_df) tuples.
            section_name is empty string for single-section sheets.
            Returns empty list if sheet has no valid data.

        Raises:
            SheetProcessingError: If sheet cannot be processed
        """
        logger.info(f"Processing sheet: {sheet_name}")

        try:
            # Read the sheet with minimal processing to preserve data
            # dtype=str prevents data corruption (dates, leading zeros, scientific notation)
            df = pd.read_excel(
                self.excel_file,
                sheet_name=sheet_name,
                header=None,  # Don't infer headers - we'll detect them
                dtype=str,  # Read everything as strings initially
                na_filter=True,  # Keep NaN for empty cells
                keep_default_na=True
            )

            # Replace NaN with empty strings for consistent handling
            df = df.fillna('')

            if df.empty:
                logger.warning(f"Sheet '{sheet_name}' is empty, skipping")
                return []

            # Split into sections and clean each
            sections = self.split_into_sections(df)

            if not sections:
                logger.warning(f"Sheet '{sheet_name}' has no data after cleaning, skipping")
                return []

            return sections

        except Exception as e:
            raise SheetProcessingError(
                f"Failed to process sheet '{sheet_name}': {str(e)}"
            )

    def extract_to_json(self, output_dir: Optional[str] = None) -> str:
        """
        Extract all sheets and save as a single JSON file

        Design: JSON array of records with source_sheet field. Bronze layer - raw replica.

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

        logger.info(f"=== Starting extraction for {len(self.sheet_names)} sheets ===")

        # Process all sheets
        all_dataframes = []
        skipped_sheets = []
        total_sections = 0

        for sheet_name in self.sheet_names:
            try:
                sections = self.process_sheet(sheet_name)

                if not sections:
                    skipped_sheets.append(sheet_name)
                    continue

                # Process each section
                for section_name, df in sections:
                    # Build full source identifier
                    # If section_name is empty, just use sheet_name (single-section sheet)
                    # Otherwise, append section name with delimiter
                    if section_name:
                        full_source = f"{sheet_name}{SECTION_DELIMITER}{section_name}"
                    else:
                        full_source = sheet_name

                    # Add source_sheet column as first column
                    df.insert(0, 'source_sheet', full_source)
                    all_dataframes.append(df)
                    total_sections += 1

            except SheetProcessingError as e:
                logger.error(f"Error processing sheet '{sheet_name}': {e}")
                skipped_sheets.append(sheet_name)
                continue

        if skipped_sheets:
            logger.info(f"Skipped {len(skipped_sheets)} sheets (empty or errors): {skipped_sheets}")

        logger.info(f"Processed {total_sections} section(s) from {len(self.sheet_names) - len(skipped_sheets)} sheet(s)")

        if not all_dataframes:
            raise NoValidSheetsError(
                f"No sheets could be processed. All {len(self.sheet_names)} sheets were empty or had errors."
            )

        # Combine all dataframes
        combined_df = pd.concat(all_dataframes, ignore_index=True)

        # Remove columns that are 100% empty (no data in any row)
        # Note: We use empty strings ('') not NaN because we fillna('') during sheet processing
        # This reduces noise and token usage for Stage 2 LLM
        columns_before = len(combined_df.columns)

        # Keep columns where at least one cell has non-empty data
        non_empty_mask = (combined_df != '').any(axis=0)
        empty_cols = combined_df.columns[~non_empty_mask].tolist()

        if empty_cols:
            logger.info(f"Found {len(empty_cols)} all-empty columns: {empty_cols[:5]}...")  # Show first 5

        non_empty_cols = combined_df.columns[non_empty_mask]
        combined_df = combined_df[non_empty_cols]

        columns_after = len(combined_df.columns)

        if columns_before > columns_after:
            logger.info(f"Removed {columns_before - columns_after} empty columns ({columns_before} → {columns_after})")

        # Save single JSON
        output_filename = f"{base_filename}.json"
        output_filepath = output_path / output_filename
        combined_df.to_json(output_filepath, orient='records', indent=2)

        logger.info(f"=== Extraction complete ===")
        logger.info(f"Output: {output_filepath}")
        logger.info(f"Total rows: {len(combined_df):,}")
        logger.info(f"Total columns: {len(combined_df.columns)}")
        logger.info(f"Total sections: {total_sections} (from {len(self.sheet_names) - len(skipped_sheets)} sheets)")

        return str(output_filepath)


def main():
    """
    Example usage of ExcelExtractor
    """
    import sys

    if len(sys.argv) < 2:
        print("Usage: python excel_extractor.py <path_to_excel_file>")
        print("Example: python excel_extractor.py ../../data/raw/goodman_2025.xlsx")
        sys.exit(1)

    file_path = sys.argv[1]

    try:
        extractor = ExcelExtractor(file_path)
        output_path = extractor.extract_to_json()

        print("\n=== Extraction Complete ===")
        print(f"Output JSON: {output_path}")
        print("\nReady for Stage 2 (LLM Transformer)")

    except ExtractorFileNotFoundError as e:
        print(f"\nError: {e}")
        print("Please check the file path and try again.")
        sys.exit(1)

    except InvalidFileFormatError as e:
        print(f"\nError: {e}")
        print("Supported formats: .xlsx, .xls, .xlsm, .xlsb")
        sys.exit(1)

    except NoValidSheetsError as e:
        print(f"\nError: {e}")
        print("The Excel file does not contain any processable sheets.")
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
