"""
Matcher module for merging AHRI data into system attributes
"""

import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


def _ensure_json_serializable(value: Any) -> Any:
    """
    Convert value to JSON-serializable Python native type.

    Handles numpy/pandas types that aren't JSON serializable.
    """
    if value is None:
        return None

    # Handle numpy/pandas integer types
    if hasattr(value, 'item'):
        return value.item()

    # Handle numpy/pandas types with dtype attribute
    if hasattr(value, 'dtype'):
        return value.item() if hasattr(value, 'item') else value

    # Already native Python type
    return value


def merge_ahri_data(system: Dict[str, Any], ahri_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge AHRI certificate data into system attributes.

    Only fills MISSING fields - preserves existing catalog data.

    Args:
        system: System dictionary from Silver JSON
        ahri_data: AHRI certificate data from scraper

    Returns:
        Updated system dictionary
    """
    attrs = system.get('system_attributes', {})

    if not attrs:
        logger.warning(f"System {system.get('system_id')} has no attributes, skipping merge")
        return system

    system_id = system.get('system_id', 'unknown')
    filled_fields = []

    # Map AHRI fields to Silver schema fields
    field_mapping = {
        'ahri_ref': 'ahri_number',
        'seer2': 'seer2',
        'eer2': 'eer2',
        'hspf2': 'hspf2',
        'capacity': 'capacity_btu',
        'tonnage': 'tonnage'
    }

    for ahri_field, silver_field in field_mapping.items():
        # Only fill if field is missing and AHRI has data
        if attrs.get(silver_field) is None and ahri_data.get(ahri_field) is not None:
            # Ensure value is JSON-serializable
            value = _ensure_json_serializable(ahri_data[ahri_field])
            attrs[silver_field] = value
            filled_fields.append(silver_field)
            logger.debug(f"System {system_id}: Filled {silver_field} = {value}")

    if filled_fields:
        logger.info(f"System {system_id}: Enriched with fields: {', '.join(filled_fields)}")
    else:
        logger.info(f"System {system_id}: No new fields filled (all present or no AHRI data)")

    system['system_attributes'] = attrs

    # Add enrichment metadata
    if 'metadata' not in system:
        system['metadata'] = {}

    if 'notes' not in system['metadata']:
        system['metadata']['notes'] = []

    if filled_fields:
        enrichment_note = f"AHRI enrichment: Added {', '.join(filled_fields)} from AHRI certificate {ahri_data.get('ahri_ref', 'unknown')}"
        system['metadata']['notes'].append(enrichment_note)

    return system


def calculate_tonnage_from_capacity(capacity_btu: Optional[int]) -> Optional[float]:
    """
    Calculate tonnage from BTU capacity.

    Formula: Tons = BTU / 12000

    Args:
        capacity_btu: Capacity in BTU

    Returns:
        Tonnage or None if capacity is None
    """
    if capacity_btu is None:
        return None

    try:
        tonnage = round(capacity_btu / 12000, 1)
        return tonnage
    except (TypeError, ValueError):
        logger.error(f"Invalid capacity_btu: {capacity_btu}")
        return None


def extract_ahri_data_from_certificate(certificate_row: Dict[str, Any], seer2_col: str) -> Dict[str, Any]:
    """
    Extract relevant AHRI data from certificate row.

    This maps the columns from the downloaded AHRI Excel file to our format.

    Args:
        certificate_row: Row from AHRI certificate Excel file
        seer2_col: Name of the SEER2 column (varies)

    Returns:
        Dictionary with standardized AHRI data
    """
    # Extract capacity and calculate tonnage
    capacity = certificate_row.get('AHRI CERTIFIED RATINGS - Cooling Capacity (95F), btuh (Appendix M1)')
    tonnage = calculate_tonnage_from_capacity(capacity) if capacity else None

    return {
        'ahri_ref': certificate_row.get('AHRI Ref. #'),
        'seer2': certificate_row.get(seer2_col),
        'eer2': certificate_row.get('AHRI CERTIFIED RATINGS - EER2 (95F) (Appendix M1)'),
        'hspf2': certificate_row.get('AHRI CERTIFIED RATINGS - HSPF2 (Region IV) (Appendix M1)'),
        'capacity': capacity,
        'tonnage': tonnage,
        'indoor_model': certificate_row.get('Indoor Unit Model Number'),
        'furnace_model': certificate_row.get('Furnace Model Number'),
    }
