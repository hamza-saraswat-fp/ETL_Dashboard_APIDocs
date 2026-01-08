"""
Validator module for determining if systems need AHRI enrichment
"""

import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


def needs_enrichment(system: Dict[str, Any]) -> bool:
    """
    Check if a system needs AHRI enrichment.

    A system needs enrichment if it's missing any of:
    - AHRI number
    - Tonnage
    - SEER2
    - Total price

    Args:
        system: System dictionary from Silver JSON

    Returns:
        True if system needs enrichment, False otherwise
    """
    attrs = system.get('system_attributes')

    # Skip systems without attributes (standalone components)
    if not attrs or attrs is None:
        return False

    # Check for missing critical fields
    missing_ahri = attrs.get('ahri_number') is None
    missing_tonnage = attrs.get('tonnage') is None
    missing_seer2 = attrs.get('seer2') is None
    missing_price = attrs.get('total_price') is None

    needs_enrich = missing_ahri or missing_tonnage or missing_seer2 or missing_price

    if needs_enrich:
        system_id = system.get('system_id', 'unknown')
        missing_fields = []
        if missing_ahri:
            missing_fields.append('ahri_number')
        if missing_tonnage:
            missing_fields.append('tonnage')
        if missing_seer2:
            missing_fields.append('seer2')
        if missing_price:
            missing_fields.append('total_price')

        logger.debug(f"System {system_id} needs enrichment (missing: {', '.join(missing_fields)})")

    return needs_enrich


def get_enrichment_priority(system: Dict[str, Any]) -> str:
    """
    Determine enrichment priority/method for a system.

    Priority:
    1. If AHRI number exists → search by AHRI number
    2. If no AHRI number → search by outdoor + indoor models

    Args:
        system: System dictionary from Silver JSON

    Returns:
        'ahri_number' or 'models'
    """
    attrs = system.get('system_attributes', {})

    if attrs and attrs.get('ahri_number'):
        return 'ahri_number'

    return 'models'
