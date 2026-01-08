"""
Taxonomy Classifier v2.0 - Categorizes HVAC systems and standalone components
"""
import json
from typing import Dict, List, Optional, Tuple
from .config import TAXONOMY_PATH


class TaxonomyClassifier:
    """Classifies HVAC systems and components based on taxonomy v2.0"""

    def __init__(self, taxonomy_path: str = TAXONOMY_PATH):
        """Initialize classifier with taxonomy configuration"""
        with open(taxonomy_path, 'r') as f:
            self.taxonomy = json.load(f)

        self.systems_config = self.taxonomy.get('systems', {})
        self.components_config = self.taxonomy.get('components', {})
        self.filters_config = self.taxonomy.get('filters', {})

    def classify_system(self, system: Dict) -> List[str]:
        """
        Classify a system or component into taxonomy categories

        Args:
            system: System dict from silver JSON

        Returns:
            List of category display names [level1, level2, ...]
        """
        # Determine if this is a complete system or standalone component
        if self._is_complete_system(system):
            return self._classify_complete_system(system)
        else:
            return self._classify_standalone_component(system)

    def _is_complete_system(self, system: Dict) -> bool:
        """
        Determine if this is a complete HVAC system

        A complete system has:
        - Valid system_attributes dict
        - Key attributes like tonnage, system_type, or stages
        - An outdoor unit component (ODU)
        """
        attrs = system.get('system_attributes')

        if not isinstance(attrs, dict) or not attrs:
            return False

        # Check for outdoor unit component - REQUIRED for complete systems
        components = system.get('components', [])
        has_outdoor_unit = any(
            c.get('component_type') == 'ODU'
            for c in components
            if isinstance(c, dict)
        )

        # If no outdoor unit, this is a standalone component
        if not has_outdoor_unit:
            return False

        # Check for key system attributes
        has_tonnage = attrs.get('tonnage') is not None and attrs.get('tonnage') != 0
        has_system_type = attrs.get('system_type') is not None
        has_stages = attrs.get('stages') is not None
        has_seer = attrs.get('seer2') is not None or attrs.get('seer') is not None

        # If has ODU + at least 2 key attributes, it's a complete system
        key_attrs_count = sum([has_tonnage, has_system_type, has_stages, has_seer])

        return key_attrs_count >= 2

    def _classify_complete_system(self, system: Dict) -> List[str]:
        """Classify a complete HVAC system"""
        # Level 1: System Type
        level1 = self._classify_system_type(system)

        # Level 2: Staging (v3.0 simplified)
        level2 = self._classify_staging(system)

        return [level1, level2]

    def _classify_standalone_component(self, system: Dict) -> List[str]:
        """Classify a standalone component"""
        components = system.get('components', [])
        if not isinstance(components, list) or not components:
            return ["-", "-"]

        # Take first component
        component = components[0]
        if not isinstance(component, dict):
            return ["-", "-"]

        # Level 1: Component Category
        level1 = self._classify_component_category(component)

        # Level 2: Component Subcategory
        level2 = self._classify_component_subcategory(component, level1)

        return [level1, level2]

    def _classify_system_type(self, system: Dict) -> str:
        """Classify Level 1 system type using priority logic (v3.0)"""
        attrs = system.get('system_attributes') or {}
        if not isinstance(attrs, dict):
            attrs = {}

        components = system.get('components', [])
        if not isinstance(components, list):
            components = []

        # Extract key attributes
        system_type = str(attrs.get('system_type') or '').upper()
        configuration = str(attrs.get('configuration') or '').lower()
        has_hspf = attrs.get('hspf2') is not None or attrs.get('hspf') is not None

        # Check component types
        has_furnace = any(
            c.get('component_type') == 'Furnace'
            for c in components if isinstance(c, dict)
        )
        has_idu = any(
            c.get('component_type') == 'IDU'
            for c in components if isinstance(c, dict)
        )

        # Apply classification priority (v3.0 - granular packaged types)

        # 1. Packaged Units (v3.0: split into AC/HP/Gas)
        if 'package' in configuration or 'packaged' in configuration:
            # Determine packaged type
            is_heat_pump = system_type == 'HP' or has_hspf or 'heat pump' in system_type.lower()

            if is_heat_pump:
                return 'Packaged Heat Pump'
            elif has_furnace:
                return 'Packaged Gas/Electric'
            else:
                return 'Packaged AC'

        # 2. Ductless systems
        if 'ductless' in system_type.lower() or 'mini' in system_type.lower() or has_idu:
            idu_count = sum(
                1 for c in components
                if isinstance(c, dict) and c.get('component_type') == 'IDU'
            )
            if idu_count > 1:
                return 'Ductless Multi Zone'
            else:
                return 'Ductless Single Zone'

        # 3. Heat Pump systems
        is_heat_pump = system_type == 'HP' or has_hspf or 'heat pump' in system_type.lower()

        if is_heat_pump:
            if has_furnace:
                return 'Split Dual Fuel'
            else:
                return 'Split Heat Pump'

        # 4. AC systems
        if has_furnace:
            return 'Split Gas/Electric'
        else:
            return 'Split Electric AC'

    def _classify_staging(self, system: Dict) -> str:
        """
        Classify Level 2 staging for complete systems (v3.0)
        Returns: "Single Stage", "Two Stage", or "Variable Speed"
        """
        attrs = system.get('system_attributes') or {}
        if not isinstance(attrs, dict):
            attrs = {}

        stages = str(attrs.get('stages') or '').lower()

        # Map stages to display names
        if 'single' in stages or '1' in stages:
            return 'Single Stage'
        elif 'two' in stages or '2' in stages:
            return 'Two Stage'
        elif 'variable' in stages or 'inverter' in stages or 'modulating' in stages:
            return 'Variable Speed'
        else:
            # Default based on SEER2 if available
            seer2 = attrs.get('seer2')
            if seer2:
                if seer2 >= 20:
                    return 'Variable Speed'
                elif seer2 >= 17:
                    return 'Two Stage'
            return 'Single Stage'

    def _classify_component_category(self, component: Dict) -> str:
        """Classify standalone component into Level 1 category"""
        comp_type = str(component.get('component_type') or '')

        # Map component_type to category using taxonomy
        categories = self.components_config.get('level_1', {}).get('categories', [])

        for category in categories:
            if comp_type in category.get('component_types', []):
                return category.get('display_name', '-')

        # Default to Accessories
        return 'Accessories'

    def _classify_component_subcategory(self, component: Dict, level1: str) -> str:
        """Classify standalone component into Level 2 subcategory"""
        # Get subcategories for this component category
        subcats_config = self.components_config.get('level_2', {}).get('subcategories', {})

        # Map level1 display name to config key
        level1_key_map = {
            'Condensers': 'condensers',
            'Air Handlers': 'air_handlers',
            'Furnaces': 'furnaces',
            'Evaporator Coils': 'evaporator_coils',
            'Ductless Indoor Units': 'ductless_indoor',
            'Heat Kits': 'default',
            'Thermostats & Controls': 'default',
            'Accessories': 'default'
        }

        config_key = level1_key_map.get(level1, 'default')
        subcats = subcats_config.get(config_key, subcats_config.get('default', []))

        # Apply specific logic based on component type
        if level1 == 'Condensers':
            return self._classify_condenser_subcat(component)
        elif level1 == 'Air Handlers':
            return self._classify_air_handler_subcat(component)
        elif level1 == 'Furnaces':
            return self._classify_furnace_subcat(component)
        elif level1 == 'Evaporator Coils':
            return self._classify_coil_subcat(component)
        else:
            # Default subcategory
            if subcats:
                return subcats[0].get('display_name', 'Miscellaneous')
            return 'Miscellaneous'

    def _classify_condenser_subcat(self, component: Dict) -> str:
        """Classify condenser into AC vs Heat Pump"""
        model = str(component.get('model_number') or '').upper()
        description = str(component.get('description') or '').lower()

        # Check for heat pump indicators
        if 'HP' in model or 'heat pump' in description or 'heating' in description:
            return 'Heat Pump Condensers'
        else:
            return 'AC Condensers'

    def _classify_air_handler_subcat(self, component: Dict) -> str:
        """Classify air handler by speed type"""
        description = str(component.get('description') or '').lower()
        model = str(component.get('model_number') or '').lower()

        # Check for variable speed indicators
        if 'variable' in description or 'vs' in model or 'ecm' in description:
            return 'Variable Speed Air Handlers'
        elif 'multi' in description:
            return 'Multi-Speed Air Handlers'
        else:
            return 'Single Speed Air Handlers'

    def _classify_furnace_subcat(self, component: Dict) -> str:
        """Classify furnace by AFUE efficiency"""
        specs = component.get('specifications') or {}
        if not isinstance(specs, dict):
            specs = {}

        afue = specs.get('afue')

        if afue is not None:
            if afue >= 95:
                return 'High Efficiency (95%+ AFUE)'
            else:
                return 'Standard Efficiency (80% AFUE)'

        # Try to infer from model number
        model = str(component.get('model_number') or '')
        if '95' in model or '96' in model or '97' in model or '98' in model:
            return 'High Efficiency (95%+ AFUE)'
        elif '80' in model:
            return 'Standard Efficiency (80% AFUE)'

        # Default to standard efficiency
        return 'Standard Efficiency (80% AFUE)'

    def _classify_coil_subcat(self, component: Dict) -> str:
        """Classify evaporator coil as cased vs uncased"""
        description = str(component.get('description') or '').lower()

        if 'cased' in description or 'cabinet' in description:
            return 'Cased Coils'
        elif 'uncased' in description or 'bare' in description:
            return 'Uncased Coils'
        else:
            # Default to cased
            return 'Cased Coils'

    def build_category_string(self, system: Dict) -> str:
        """
        Build a complete category string for a system

        For complete systems: Returns Level 2 pattern string
        For components: Returns Level 2 subcategory

        Returns:
            Category string like "GXV6 Two Stage HP Upflow" or "High Efficiency (95%+ AFUE)"
        """
        categories = self.classify_system(system)

        if len(categories) >= 2:
            return categories[1]
        elif len(categories) == 1:
            return categories[0]
        else:
            return '-'
