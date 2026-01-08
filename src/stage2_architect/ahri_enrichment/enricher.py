"""
Main AHRI Enricher - Orchestrates enrichment of Silver JSON systems
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path

from .validator import needs_enrichment, get_enrichment_priority
from .matcher import merge_ahri_data
from .playwright_scraper import PlaywrightAHRIScraper

logger = logging.getLogger(__name__)


class AHRIEnricher:
    """
    AHRI Enricher for Silver JSON systems.

    Enriches systems with missing AHRI data by:
    1. Checking which systems need enrichment
    2. Scraping AHRI directory for missing data
    3. Merging data back into systems
    """

    def __init__(self, cache_dir: str = "./cache/ahri", concurrency: int = 5, headless: bool = True):
        """
        Initialize enricher.

        Args:
            cache_dir: Directory for caching AHRI downloads
            concurrency: Max concurrent browser contexts
            headless: Run browser in headless mode
        """
        self.scraper = PlaywrightAHRIScraper(
            cache_dir=cache_dir,
            concurrency=concurrency,
            headless=headless
        )
        logger.info("AHRI Enricher initialized")

    def enrich_systems(self, systems: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Enrich systems with missing AHRI data.

        Args:
            systems: List of system dictionaries from Silver JSON

        Returns:
            List of enriched systems
        """
        logger.info(f"=== AHRI Enrichment Starting ===")
        logger.info(f"Total systems: {len(systems)}")

        # Filter systems that need enrichment
        systems_to_enrich = [s for s in systems if needs_enrichment(s)]
        logger.info(f"Systems needing enrichment: {len(systems_to_enrich)}")

        if not systems_to_enrich:
            logger.info("No systems need enrichment")
            return systems

        # Enrich each system
        enriched_systems = []
        for system in systems:
            if needs_enrichment(system):
                enriched_system = self._enrich_system(system)
                enriched_systems.append(enriched_system)
            else:
                enriched_systems.append(system)

        enriched_count = sum(1 for s in enriched_systems if not needs_enrichment(s))
        logger.info(f"=== AHRI Enrichment Complete ===")
        logger.info(f"Successfully enriched: {enriched_count}/{len(systems_to_enrich)} systems")

        return enriched_systems

    def _enrich_system(self, system: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich a single system with AHRI data.

        Args:
            system: System dictionary from Silver JSON

        Returns:
            Enriched system
        """
        system_id = system.get('system_id', 'unknown')
        logger.info(f"Enriching system {system_id}")

        # Get component models
        outdoor_model = self._get_outdoor_unit(system)
        indoor_model = self._get_indoor_unit(system)
        furnace_model = self._get_furnace_unit(system)

        if not outdoor_model:
            logger.warning(f"System {system_id}: No outdoor unit found, cannot enrich")
            return system

        logger.info(f"System {system_id}: outdoor={outdoor_model}, indoor={indoor_model}")

        # Determine enrichment priority
        priority = get_enrichment_priority(system)
        attrs = system.get('system_attributes', {})

        ahri_data = None

        # Priority 1: Search by AHRI number if exists
        if priority == 'ahri_number':
            ahri_number = attrs.get('ahri_number')
            logger.info(f"System {system_id}: Searching by AHRI# {ahri_number}")

            # AHRI# search returns dict directly (scrapes details page)
            ahri_data = asyncio.run(self.scraper.search_by_ahri_number(ahri_number))

        # Priority 2: Search by outdoor + indoor models using ADVANCED SEARCH
        else:
            # Get system type for AHRI program selection
            system_type = attrs.get('system_type', 'AC') if attrs else 'AC'

            # NEW: Try advanced search with both outdoor and indoor models first
            if indoor_model:
                logger.info(f"System {system_id}: Trying advanced search (outdoor={outdoor_model}, indoor={indoor_model}, type={system_type})")

                ahri_file = asyncio.run(self.scraper.search_by_outdoor_and_indoor_models(
                    outdoor_model=outdoor_model,
                    indoor_model=indoor_model,
                    system_type=system_type,
                    furnace_model=furnace_model
                ))

                if ahri_file:
                    # Match specific indoor unit
                    ahri_data = self.scraper.match_indoor_unit(outdoor_model, indoor_model, ahri_file)

            # FALLBACK: If advanced search fails or no indoor model, use old outdoor-only search
            if not ahri_data:
                logger.info(f"System {system_id}: Falling back to outdoor-only search")

                ahri_file = asyncio.run(self.scraper.search_by_outdoor_model(outdoor_model))
                if ahri_file:
                    # Match specific indoor unit
                    ahri_data = self.scraper.match_indoor_unit(outdoor_model, indoor_model, ahri_file)

        # Merge AHRI data if found
        if ahri_data:
            logger.info(f"System {system_id}: AHRI data found, merging")
            system = merge_ahri_data(system, ahri_data)
        else:
            logger.warning(f"System {system_id}: No AHRI data found")

        return system

    def _get_outdoor_unit(self, system: Dict[str, Any]) -> Optional[str]:
        """
        Extract outdoor unit model number from system components.

        Args:
            system: System dictionary

        Returns:
            Outdoor unit model number or None
        """
        components = system.get('components', [])

        for component in components:
            comp_type = component.get('component_type', '')
            if comp_type == 'ODU':
                model = component.get('model_number')
                if model:
                    return model.strip().upper()

        return None

    def _get_indoor_unit(self, system: Dict[str, Any]) -> Optional[str]:
        """
        Extract indoor unit model number from system components.

        Prioritizes in order: IDU, Coil, AirHandler, Furnace

        Args:
            system: System dictionary

        Returns:
            Indoor unit model number or None
        """
        components = system.get('components', [])

        # Priority order for indoor units
        priority_order = ['IDU', 'Coil', 'AirHandler', 'Furnace']

        for comp_type in priority_order:
            for component in components:
                if component.get('component_type') == comp_type:
                    model = component.get('model_number')
                    if model:
                        return model.strip().upper()

        return None

    def _get_furnace_unit(self, system: Dict[str, Any]) -> Optional[str]:
        """
        Extract furnace model number from system components.

        Args:
            system: System dictionary

        Returns:
            Furnace model number or None
        """
        components = system.get('components', [])

        for component in components:
            comp_type = component.get('component_type', '')
            if comp_type == 'Furnace':
                model = component.get('model_number')
                if model:
                    return model.strip().upper()

        return None
