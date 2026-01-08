"""
Stage 2: The Architect - LLM-based data transformation

This module uses Claude Sonnet 4.5 for two approaches:
1. Direct transformation: Bronze JSON → Silver JSON (NEW, recommended)
2. Code generation: Generate Python transformer code (legacy)
"""

# NEW: Direct JSON transformation (recommended approach)
from .bronze_json_transformer import BronzeJSONTransformer
from .batchers import (
    batch_by_sheet, get_sheet_stats, batch_large_sheet,
    batch_by_table, get_table_stats, batch_large_table
)
from .silver_validator import SilverValidator, validate_silver
from .classifiers import (
    should_skip_sheet, classify_sheets,
    should_skip_table, classify_tables
)
from .pipelines import process_excel_bronze, process_pdf_bronze
from .llm_client import LLMClient

# LEGACY: Code generation approach
from .architect import Architect
from .sampler import CSVSampler

__all__ = [
    # New transformation classes
    'BronzeJSONTransformer',
    # Excel batchers
    'batch_by_sheet',
    'get_sheet_stats',
    'batch_large_sheet',
    # PDF batchers
    'batch_by_table',
    'get_table_stats',
    'batch_large_table',
    # Validators
    'SilverValidator',
    'validate_silver',
    # Excel classifiers
    'should_skip_sheet',
    'classify_sheets',
    # PDF classifiers
    'should_skip_table',
    'classify_tables',
    # Pipelines
    'process_excel_bronze',
    'process_pdf_bronze',
    # LLM client
    'LLMClient',
    # Legacy classes
    'Architect',
    'CSVSampler'
]
