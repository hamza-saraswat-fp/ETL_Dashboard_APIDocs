"""
Excel Formatter - Formats silver JSON data into Excel structure
"""
from typing import Dict, List, Optional
from .taxonomy_classifier import TaxonomyClassifier
from .config import (
    COMPONENT_TYPE_DISPLAY,
    SYSTEM_TYPE_DISPLAY,
    STAGES_DISPLAY,
    DEFAULT_APPLY_TAX,
    DEFAULT_QUANTITY,
    DEFAULT_PRODUCT_OR_SERVICE,
    DEFAULT_PRICEBOOK_CATEGORY,
    ORIENTATION_DISPLAY,
    COMPONENT_DESC_DISPLAY
)


class ExcelFormatter:
    """Formats system data into Excel rows"""

    def __init__(self, costbook_title: str = "WinSupply"):
        """
        Initialize formatter

        Args:
            costbook_title: The costbook title for all items
        """
        self.costbook_title = costbook_title
        self.classifier = TaxonomyClassifier()

    def _safe_get_specs(self, component: Dict) -> Dict:
        """
        Safely get specifications dict from component.

        Handles cases where specifications might be:
        - None -> returns empty dict
        - String (e.g., dimensions like "27X26X26") -> returns empty dict
        - Dict -> returns the dict

        Args:
            component: Component dict from silver JSON

        Returns:
            Dict (never None, never non-dict type)
        """
        specs = component.get('specifications')
        if specs is None or not isinstance(specs, dict):
            return {}
        return specs

    # ==================== Item Description Builder Methods ====================

    def _build_item_description(
        self,
        component: Dict,
        system_attrs: Optional[Dict] = None
    ) -> str:
        """
        Build a rich 2-3 line description for a component.

        Args:
            component: Component dict from silver JSON
            system_attrs: System attributes (optional, for system components)

        Returns:
            Multi-line description string
        """
        lines = []
        attrs = system_attrs or {}
        specs = self._safe_get_specs(component)
        comp_type = component.get('component_type', '')
        model = component.get('model_number', '')

        # Line 1: Component type with capacity info
        line1 = self._build_description_line1(component, attrs, specs)
        if line1:
            lines.append(line1)

        # Line 2: Efficiency ratings
        line2 = self._build_description_line2(component, attrs, specs)
        if line2:
            lines.append(line2)

        # Line 3: Configuration and model
        line3 = self._build_description_line3(component, attrs, specs)
        if line3:
            lines.append(line3)

        # Fallback: if no lines generated, use basic description
        if not lines:
            comp_display = COMPONENT_DESC_DISPLAY.get(comp_type, comp_type)
            return f"{comp_display}\nModel: {model}" if model else comp_display

        return "\n".join(lines)

    def _build_description_line1(
        self,
        component: Dict,
        attrs: Dict,
        specs: Dict
    ) -> str:
        """Build line 1: Component type with capacity info"""
        comp_type = component.get('component_type', '')

        # Determine display name based on component type
        if comp_type == 'ODU':
            display_name = "Heat Pump" if self._is_heat_pump_odu(component, attrs) else "AC Condenser"
        elif comp_type == 'Furnace':
            fuel_type = self._infer_furnace_fuel_type(component, specs)
            display_name = f"{fuel_type} Furnace" if fuel_type else "Furnace"
        elif comp_type == 'Coil':
            coil_type = self._infer_coil_type(component)
            display_name = f"{coil_type} Evaporator Coil" if coil_type else "Evaporator Coil"
        else:
            display_name = COMPONENT_DESC_DISPLAY.get(comp_type, comp_type)

        # Build capacity info
        parts = [display_name]

        # Add tonnage
        tonnage = attrs.get('tonnage') or specs.get('tonnage')
        if tonnage:
            parts.append(f"{tonnage} Ton")

        # Add BTU capacity
        capacity = attrs.get('capacity_btu') or specs.get('capacity_btu')
        if capacity:
            try:
                parts.append(f"{int(capacity):,} BTU")
            except (ValueError, TypeError):
                pass

        return " - ".join(parts) if len(parts) > 1 else parts[0]

    def _build_description_line2(
        self,
        component: Dict,
        attrs: Dict,
        specs: Dict
    ) -> str:
        """Build line 2: Efficiency ratings"""
        comp_type = component.get('component_type', '')
        efficiency_parts = []

        if comp_type == 'ODU':
            # SEER2/SEER
            seer = attrs.get('seer2') or attrs.get('seer') or specs.get('seer2') or specs.get('seer')
            if seer:
                label = "SEER2" if (attrs.get('seer2') or specs.get('seer2')) else "SEER"
                efficiency_parts.append(f"{label}: {seer}")

            # HSPF2/HSPF (for heat pumps)
            if self._is_heat_pump_odu(component, attrs):
                hspf = attrs.get('hspf2') or attrs.get('hspf') or specs.get('hspf2') or specs.get('hspf')
                if hspf:
                    label = "HSPF2" if (attrs.get('hspf2') or specs.get('hspf2')) else "HSPF"
                    efficiency_parts.append(f"{label}: {hspf}")

            # EER2/EER
            eer = attrs.get('eer2') or attrs.get('eer') or specs.get('eer2') or specs.get('eer')
            if eer:
                label = "EER2" if (attrs.get('eer2') or specs.get('eer2')) else "EER"
                efficiency_parts.append(f"{label}: {eer}")

        elif comp_type == 'Furnace':
            # AFUE
            afue = specs.get('afue') or self._infer_afue_from_model(component.get('model_number', ''))
            if afue:
                efficiency_parts.append(f"{afue}% AFUE")

            # Stages
            stages = self._format_stages_display(attrs.get('stages') or specs.get('stages'))
            if stages:
                efficiency_parts.append(stages)

        elif comp_type == 'Coil':
            # For coils, show system compatibility
            stages = self._format_stages_display(attrs.get('stages'))
            if stages:
                return f"For use with {stages.lower()} systems"

        elif comp_type in ('AHU', 'AirHandler', 'Air Handler'):
            # Air handler speed type
            speed_type = self._infer_air_handler_speed(component)
            if speed_type:
                return speed_type

        return " | ".join(efficiency_parts) if efficiency_parts else ""

    def _build_description_line3(
        self,
        component: Dict,
        attrs: Dict,
        specs: Dict
    ) -> str:
        """Build line 3: Configuration and model number"""
        parts = []
        model = component.get('model_number', '')
        comp_type = component.get('component_type', '')

        # Add stages for ODU
        if comp_type == 'ODU':
            stages = self._format_stages_display(attrs.get('stages'))
            if stages:
                parts.append(stages)

        # Add orientation for furnaces, coils, air handlers
        if comp_type in ('Furnace', 'Coil', 'AHU', 'AirHandler', 'Air Handler'):
            orientation = self._infer_orientation_from_model(model) or specs.get('orientation')
            if orientation:
                # Convert to display format
                orientation_display = ORIENTATION_DISPLAY.get(orientation.lower(), orientation.title())
                parts.append(orientation_display)

        # Add voltage if available
        voltage = attrs.get('voltage') or specs.get('voltage')
        if voltage and comp_type == 'ODU':
            parts.append(voltage)

        # Always add model number
        if model:
            parts.append(f"Model: {model}")

        return " | ".join(parts) if parts else ""

    # ==================== Inference Helper Methods ====================

    def _is_heat_pump_odu(self, component: Dict, system_attrs: Dict) -> bool:
        """Determine if ODU is a heat pump"""
        system_type = str(system_attrs.get('system_type') or '').upper()
        has_hspf = system_attrs.get('hspf2') or system_attrs.get('hspf')
        model = str(component.get('model_number') or '').upper()
        description = str(component.get('description') or '').lower()

        return (
            system_type in ('HP', 'HEAT PUMP', 'HEATPUMP') or
            has_hspf or
            'heat pump' in description or
            model.startswith('GSZ') or  # Goodman heat pump prefix
            model.startswith('ASZ')     # Amana heat pump prefix
        )

    def _format_stages_display(self, stages: Optional[str]) -> str:
        """Format stages value for description display"""
        if not stages:
            return ""
        return STAGES_DISPLAY.get(str(stages).lower(), str(stages).title())

    def _infer_furnace_fuel_type(self, component: Dict, specs: Dict) -> str:
        """Infer furnace fuel type from model number or specs"""
        if specs.get('fuel_type'):
            return specs['fuel_type'].title()

        model = str(component.get('model_number') or '').upper()
        description = str(component.get('description') or '').lower()

        if 'electric' in description:
            return "Electric"
        if 'gas' in description:
            return "Gas"

        # Model number patterns
        if model.startswith('G') or 'GAS' in model:
            return "Gas"
        if model.startswith('E') and not model.startswith('EV'):
            return "Electric"

        return "Gas"  # Default to gas

    def _infer_afue_from_model(self, model: str) -> Optional[int]:
        """Infer AFUE percentage from model number"""
        model_upper = str(model).upper()

        # Look for common AFUE patterns in model numbers
        if '97' in model_upper or '98' in model_upper:
            return 97
        if '96' in model_upper:
            return 96
        if '95' in model_upper:
            return 95
        if '92' in model_upper:
            return 92
        if '80' in model_upper:
            return 80

        return None

    def _infer_orientation_from_model(self, model: str) -> str:
        """Infer orientation from model number suffix"""
        model_upper = str(model).upper()

        # Check suffixes and common patterns
        if model_upper.endswith('U') or '-U' in model_upper or 'U-' in model_upper:
            return "Upflow"
        if model_upper.endswith('D') or '-D' in model_upper or 'D-' in model_upper:
            return "Downflow"
        if model_upper.endswith('H') or '-H' in model_upper or 'H-' in model_upper:
            return "Horizontal"
        if model_upper.endswith('M') or '-M' in model_upper or 'M-' in model_upper:
            return "Multi-Position"

        return ""

    def _infer_coil_type(self, component: Dict) -> str:
        """Infer if coil is cased or uncased"""
        description = str(component.get('description') or '').lower()
        model = str(component.get('model_number') or '').upper()

        if 'uncased' in description or 'bare' in description:
            return "Uncased"
        if 'cased' in description or 'cabinet' in description:
            return "Cased"

        # Model number patterns - NC prefix often means cased
        if model.startswith('NC'):
            return "Cased"

        return "Cased"  # Default to cased

    def _infer_air_handler_speed(self, component: Dict) -> str:
        """Infer air handler motor/speed type"""
        description = str(component.get('description') or '').lower()
        model = str(component.get('model_number') or '').lower()

        if 'variable' in description or 'vs' in model or 'ecm' in description or 'ecm' in model:
            return "Variable Speed ECM Motor"
        if 'multi' in description or 'multi-speed' in description:
            return "Multi-Speed Motor"

        return ""

    # ==================== End Description Builder Methods ====================

    def format_system(self, system: Dict) -> List[Dict]:
        """
        Format a system into one or more Excel rows (one per component)

        Args:
            system: System dict from silver JSON

        Returns:
            List of row dicts, one per component
        """
        # Check if this is a single item (not a full system)
        if self._is_single_item(system):
            return self._format_single_item(system)

        # Format as full system
        attrs = system.get('system_attributes') or {}
        components = system.get('components', [])

        if not isinstance(attrs, dict):
            attrs = {}
        if not isinstance(components, list):
            components = []

        # Generate shared fields
        job_name = self._generate_job_name(system)
        job_description = self._generate_job_description(system)
        categories = self.classifier.classify_system(system)
        category_string = self.classifier.build_category_string(system)

        # Get custom filter values
        custom_filters = self._extract_custom_filters(system)

        # Create one row per component
        rows = []
        for component in components:
            if not isinstance(component, dict):
                continue
            row = self._create_component_row(
                component=component,
                job_name=job_name,
                job_description=job_description,
                categories=categories,
                category_string=category_string,
                custom_filters=custom_filters,
                system_attrs=attrs
            )
            rows.append(row)

        return rows

    def _generate_job_name(self, system: Dict) -> str:
        """
        Generate job name like:
        "1.5 Ton Single Stage AC System - 16.5 SEER - 216723483"
        """
        attrs = system.get('system_attributes') or {}
        if not isinstance(attrs, dict):
            attrs = {}

        tonnage = attrs.get('tonnage', 0)
        stages = str(attrs.get('stages') or 'single')
        system_type = str(attrs.get('system_type') or 'AC')
        seer2 = attrs.get('seer2', attrs.get('seer', ''))

        # Prefer AHRI number over system_id for identification
        ahri_number = attrs.get('ahri_number')
        system_id = str(ahri_number or system.get('system_id') or '')

        # Format stages
        stages_display = STAGES_DISPLAY.get(stages.lower(), stages.title())

        # Format system type
        system_type_display = SYSTEM_TYPE_DISPLAY.get(system_type, system_type)

        # Build job name - use "System" or "Component" based on classification
        is_complete = self.classifier._is_complete_system(system)
        suffix = "System" if is_complete else "Component"

        parts = [
            f"{tonnage} Ton",
            stages_display,
            f"{system_type_display} {suffix}"
        ]

        if seer2:
            parts.append(f"- {seer2} SEER")

        # Only append system_id if it's an AHRI number (not COMP_XXX)
        if system_id and not str(system_id).startswith('COMP_'):
            parts.append(f"- {system_id}")

        return " ".join(parts)

    def _generate_job_description(self, system: Dict) -> str:
        """
        Generate detailed job description with all components listed
        """
        attrs = system.get('system_attributes') or {}
        components = system.get('components', [])

        if not isinstance(attrs, dict):
            attrs = {}
        if not isinstance(components, list):
            components = []

        lines = []

        # Header with system type and staging
        system_type = str(attrs.get('system_type') or 'AC')
        stages = str(attrs.get('stages') or 'single')
        configuration = str(attrs.get('configuration') or 'split')

        stages_display = STAGES_DISPLAY.get(stages.lower(), stages.title())

        lines.append(f"{stages_display} {configuration.title()} System")
        lines.append("")

        # Components section
        lines.append("Components:")
        for comp in components:
            if not isinstance(comp, dict):
                continue
            comp_type = str(comp.get('component_type') or 'Component')
            model = str(comp.get('model_number') or 'Unknown')
            comp_display = COMPONENT_TYPE_DISPLAY.get(comp_type, comp_type)
            lines.append(f"* {comp_display}: {model}")

        lines.append("")

        # Specifications section
        lines.append("Specifications:")

        if attrs.get('tonnage'):
            lines.append(f"* Tonnage: {attrs['tonnage']}")

        if attrs.get('capacity_btu'):
            lines.append(f"* Cooling Capacity: {attrs['capacity_btu']:,} BTU")

        if attrs.get('seer2'):
            lines.append(f"* SEER2: {attrs['seer2']}")
        elif attrs.get('seer'):
            lines.append(f"* SEER: {attrs['seer']}")

        if attrs.get('eer2'):
            lines.append(f"* EER2: {attrs['eer2']}")
        elif attrs.get('eer'):
            lines.append(f"* EER: {attrs['eer']}")

        if attrs.get('hspf2'):
            lines.append(f"* HSPF2: {attrs['hspf2']}")
        elif attrs.get('hspf'):
            lines.append(f"* HSPF: {attrs['hspf']}")

        # Determine fuel source
        fuel_source = self._determine_fuel_source(system)
        if fuel_source:
            lines.append(f"* Fuel Source: {fuel_source}")

        return "\n".join(lines)

    def _create_component_row(
        self,
        component: Dict,
        job_name: str,
        job_description: str,
        categories: List[str],
        category_string: str,
        custom_filters: Dict,
        system_attrs: Optional[Dict] = None
    ) -> Dict:
        """Create a single row for a component"""

        # Format item name like "* AC: NS16A18SA5"
        comp_type = component.get('component_type', 'Component')
        comp_display = COMPONENT_TYPE_DISPLAY.get(comp_type, comp_type)
        model = component.get('model_number', '')
        item_name = f"* {comp_display}: {model}"

        # Format item description using the rich description builder
        description = self._build_item_description(component, system_attrs)

        # Get price
        price = component.get('price', '')

        # Build row dict with all 32 columns
        row = {
            "Costbook Title": self.costbook_title,
            "Job Name": job_name,
            "Job Description": job_description,
            "Item Name": item_name,
            "Item Description": description,
            "Item #/SKU": model,
            "Unit Cost": price,
            "Apply Tax": DEFAULT_APPLY_TAX,
            "Quantity": DEFAULT_QUANTITY,
            "Product or Service": DEFAULT_PRODUCT_OR_SERVICE,
        }

        # Add pricebook categories (up to 10) - v3.0 2-level taxonomy
        # Category 1: System Type or Component Category
        row["Pricebook Category 1"] = categories[0] if len(categories) > 0 else DEFAULT_PRICEBOOK_CATEGORY

        # Category 2: Staging (for systems) or Component Type (for standalone components)
        row["Pricebook Category 2"] = categories[1] if len(categories) > 1 else DEFAULT_PRICEBOOK_CATEGORY

        # Categories 3-10: Reserved for future use (Orientation, Brand, etc.)
        for i in range(3, 11):
            row[f"Pricebook Category {i}"] = DEFAULT_PRICEBOOK_CATEGORY

        # Add custom filters (12 filters)
        for i in range(1, 13):
            key = f"Custom Filter {i}"
            row[key] = custom_filters.get(key, "")

        return row

    def _extract_custom_filters(self, system: Dict) -> Dict:
        """Extract custom filter values from system attributes (taxonomy v2.0)"""
        attrs = system.get('system_attributes', {})

        # Get raw values
        tonnage = attrs.get('tonnage', '')
        capacity_btu = attrs.get('capacity_btu', '')
        seer2 = attrs.get('seer2', attrs.get('seer', ''))
        eer2 = attrs.get('eer2', attrs.get('eer', ''))
        hspf2 = attrs.get('hspf2', attrs.get('hspf', ''))

        # Round numeric values for filters only
        if tonnage and tonnage != 0:
            tonnage = round(float(tonnage))

        if seer2:
            try:
                seer2 = round(float(seer2))
            except (ValueError, TypeError):
                pass

        if eer2:
            try:
                eer2 = round(float(eer2))
            except (ValueError, TypeError):
                pass

        if hspf2:
            try:
                hspf2 = round(float(hspf2))
            except (ValueError, TypeError):
                pass

        # Convert capacity to range
        if capacity_btu:
            capacity_btu = self._capacity_to_range(capacity_btu)

        filters = {
            "Custom Filter 1": tonnage,          # Tonnage (rounded)
            "Custom Filter 2": capacity_btu,     # Capacity (ranged)
            "Custom Filter 3": seer2,            # SEER 2 (rounded)
            "Custom Filter 4": eer2,             # EER2 (rounded)
            "Custom Filter 5": hspf2,            # HSPF 2 (rounded)
            "Custom Filter 6": self._determine_fuel_source(system),  # Fuel Source
            "Custom Filter 7": self._extract_compressor_type(system),  # Compressor
            "Custom Filter 8": '',  # Reserved
            "Custom Filter 9": '',  # Reserved
            "Custom Filter 10": '',  # Reserved
            "Custom Filter 11": '',  # Reserved
            "Custom Filter 12": '',  # Reserved
        }

        return filters

    def _capacity_to_range(self, capacity_btu) -> str:
        """Convert BTU capacity to standardized range string"""
        try:
            btu = float(capacity_btu)
        except (ValueError, TypeError):
            return ''

        if btu <= 18000:
            return '12,000-18,000 BTU'
        elif btu <= 24000:
            return '18,001-24,000 BTU'
        elif btu <= 30000:
            return '24,001-30,000 BTU'
        elif btu <= 36000:
            return '30,001-36,000 BTU'
        elif btu <= 42000:
            return '36,001-42,000 BTU'
        elif btu <= 48000:
            return '42,001-48,000 BTU'
        elif btu <= 60000:
            return '48,001-60,000 BTU'
        else:
            return '60,001+ BTU'

    def _determine_fuel_source(self, system: Dict) -> str:
        """Determine fuel source from system components"""
        components = system.get('components', [])

        has_furnace = any('furnace' in c.get('component_type', '').lower() for c in components)
        has_heat_pump = any('heat pump' in str(c).lower() for c in components)

        attrs = system.get('system_attributes', {})
        system_type = (attrs.get('system_type') or '').lower()

        if 'heat pump' in system_type or has_heat_pump:
            return 'Electric'
        elif has_furnace:
            # Check model number for gas indicators
            for c in components:
                if 'furnace' in c.get('component_type', '').lower():
                    model = c.get('model_number', '').lower()
                    if 'g' in model or 'gas' in model:
                        return 'Gas'
            return 'Electric'
        else:
            return 'Electric'

    def _determine_orientation(self, system: Dict) -> str:
        """Determine orientation from component model numbers"""
        components = system.get('components', [])

        for comp in components:
            model = comp.get('model_number', '').upper()

            # Check for orientation suffixes
            if model.endswith('U') or 'U-' in model:
                return 'Upflow'
            elif model.endswith('D') or 'D-' in model:
                return 'Downflow'
            elif model.endswith('H') or 'H-' in model:
                return 'Horizontal'
            elif model.endswith('M') or 'M-' in model:
                return 'Multi-Position'

        return 'Horizontal'  # Default

    def _extract_coil_type(self, system: Dict) -> str:
        """Extract coil type from components"""
        components = system.get('components', [])

        for comp in components:
            comp_type = comp.get('component_type', '').lower()
            if 'coil' in comp_type:
                return 'Cased Coil'

        return ''

    def _extract_series(self, system: Dict) -> str:
        """Extract series from model numbers"""
        components = system.get('components', [])

        for comp in components:
            model = comp.get('model_number', '')
            if model and len(model) >= 4:
                # Extract first 4 alphanumeric characters as series
                series = model[:4].upper()
                if series.isalnum():
                    return series

        return ''

    def _extract_compressor_type(self, system: Dict) -> str:
        """Extract compressor type from stages"""
        attrs = system.get('system_attributes') or {}
        if not isinstance(attrs, dict):
            attrs = {}

        stages = str(attrs.get('stages') or '').lower()

        if 'single' in stages:
            return 'Single Stage'
        elif 'two' in stages or '2' in stages:
            return 'Two Stage'
        elif 'variable' in stages or 'inverter' in stages:
            return 'Variable Speed'
        else:
            return 'Single Stage'

    def _is_single_item(self, system: Dict) -> bool:
        """
        Determine if this is a single item (not a full system)

        A single item has:
        - Only 1 component OR
        - Missing/incomplete system_attributes (no tonnage, no stages, etc.)
        """
        components = system.get('components', [])
        if not isinstance(components, list):
            return True

        # If no components or more than 1, check attributes
        attrs = system.get('system_attributes')
        if not isinstance(attrs, dict) or not attrs:
            # Missing system attributes = single item
            return len(components) == 1

        # Check if key system attributes are missing
        has_tonnage = attrs.get('tonnage') is not None and attrs.get('tonnage') != 0
        has_stages = attrs.get('stages') is not None
        has_system_type = attrs.get('system_type') is not None

        # If missing key attributes, treat as single item
        if not (has_tonnage or has_stages or has_system_type):
            return len(components) == 1

        # If only 1 component AND missing most attributes, it's a single item
        if len(components) == 1 and sum([has_tonnage, has_stages, has_system_type]) < 2:
            return True

        return False

    def _format_single_item(self, system: Dict) -> List[Dict]:
        """
        Format a single item (not a full system)

        Format like:
        - Job Name: Simple component name (unique per system_id)
        - Job Description: blank
        - Item Name: Same as Job Name
        - Item Description: blank
        - Pricebook Category 1: General category
        - Pricebook Category 2: "Miscellaneous"
        """
        components = system.get('components', [])
        if not isinstance(components, list) or not components:
            return []

        component = components[0]  # Take first/only component
        if not isinstance(component, dict):
            return []

        # Get component info
        model = str(component.get('model_number') or '')
        comp_type = str(component.get('component_type') or 'Component')
        description = str(component.get('description') or '')
        price = component.get('price', '')
        system_id = str(system.get('system_id') or '')

        # Generate simple job/item name - prioritize model for uniqueness
        # Append system_id to ensure uniqueness (e.g., for standalone components)
        if model:
            base_name = model
        elif description:
            base_name = description
        else:
            base_name = comp_type

        # Use base_name directly - don't append COMP_XXX placeholders
        item_name = base_name

        # Generate rich item description for single items too
        item_description = self._build_item_description(component, None)

        # Build row
        row = {
            "Costbook Title": self.costbook_title,
            "Job Name": item_name,
            "Job Description": "",  # Blank for single items
            "Item Name": item_name,
            "Item Description": item_description,  # Rich description for single items
            "Item #/SKU": model,
            "Unit Cost": price,
            "Apply Tax": DEFAULT_APPLY_TAX,
            "Quantity": DEFAULT_QUANTITY,
            "Product or Service": DEFAULT_PRODUCT_OR_SERVICE,
        }

        # Use taxonomy classifier for single items (same as complete systems)
        categories = self.classifier.classify_system(system)
        category_string = self.classifier.build_category_string(system)

        # Category 1: Component category (e.g., "Furnaces", "Air Handlers")
        row["Pricebook Category 1"] = categories[0] if len(categories) > 0 else DEFAULT_PRICEBOOK_CATEGORY

        # Category 2: Component subcategory (e.g., "High Efficiency (95%+ AFUE)")
        row["Pricebook Category 2"] = categories[1] if len(categories) > 1 else DEFAULT_PRICEBOOK_CATEGORY

        # Fill remaining categories with default
        for i in range(3, 11):
            row[f"Pricebook Category {i}"] = DEFAULT_PRICEBOOK_CATEGORY

        # Add empty custom filters (no system-level specs for single items)
        for i in range(1, 13):
            row[f"Custom Filter {i}"] = ""

        return [row]
