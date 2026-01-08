"""
Smart sampling for LLM consumption
Strategy: First 10 + Middle 10 + Last 10 rows per sheet (or all if ≤30)
"""

import pandas as pd
from typing import Tuple
import logging

logger = logging.getLogger(__name__)


class CSVSampler:
    """Samples CSV data intelligently for LLM analysis"""

    def __init__(self, rows_per_section: int = 10):
        """
        Args:
            rows_per_section: Number of rows to take from start/middle/end of each sheet
        """
        self.rows_per_section = rows_per_section

    def sample(self, csv_path: str) -> Tuple[pd.DataFrame, dict]:
        """
        Sample CSV with sheet-aware logic

        Args:
            csv_path: Path to bronze layer CSV file

        Returns:
            Tuple of (sampled_dataframe, sampling_metadata)
        """
        df = pd.read_csv(csv_path)

        # Check for source_sheet column
        if 'source_sheet' not in df.columns:
            logger.warning("No 'source_sheet' column found, sampling first 200 rows")
            sampled = df.head(200)
            metadata = {
                "total_rows": len(df),
                "sampled_rows": len(sampled),
                "sheets": [],
                "strategy": "simple_head"
            }
            return sampled, metadata

        # Sheet-aware sampling
        sheets = df['source_sheet'].unique()
        samples = []
        sheet_metadata = []

        logger.info(f"Found {len(sheets)} unique sheets in CSV")

        for sheet in sheets:
            sheet_df = df[df['source_sheet'] == sheet]
            sheet_size = len(sheet_df)

            if sheet_size <= 30:
                # Small sheet - take everything
                sample = sheet_df
                strategy = "all"
                logger.info(f"  📄 {sheet}: {sheet_size} rows (sending all)")
            else:
                # Large sheet - first N + middle N + last N
                first_n = sheet_df.head(self.rows_per_section)
                last_n = sheet_df.tail(self.rows_per_section)

                # Calculate middle section
                mid_start = (sheet_size - self.rows_per_section) // 2
                mid_end = mid_start + self.rows_per_section
                middle_n = sheet_df.iloc[mid_start:mid_end]

                sample = pd.concat([first_n, middle_n, last_n])
                strategy = f"first_{self.rows_per_section}_middle_{self.rows_per_section}_last_{self.rows_per_section}"
                logger.info(f"  📄 {sheet}: {sheet_size} rows (first {self.rows_per_section} + middle {self.rows_per_section} + last {self.rows_per_section})")

            samples.append(sample)
            sheet_metadata.append({
                "sheet_name": sheet,
                "total_rows": sheet_size,
                "sampled_rows": len(sample),
                "strategy": strategy
            })

        sampled_df = pd.concat(samples, ignore_index=True)

        metadata = {
            "total_rows": len(df),
            "sampled_rows": len(sampled_df),
            "sheets": sheet_metadata,
            "strategy": "sheet_aware"
        }

        logger.info(f"✅ Total sampled: {len(sampled_df)} rows from {len(sheets)} sheets")

        return sampled_df, metadata
