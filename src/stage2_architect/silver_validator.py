"""
Validator for silver layer JSON against schema
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any

logger = logging.getLogger(__name__)


class SilverValidator:
    """Validates silver layer output against schema"""

    def __init__(self, schema_path: str = None):
        """
        Initialize validator

        Args:
            schema_path: Path to silver_layer_schema_v2.json
                        If None, uses default path
        """
        if schema_path is None:
            project_root = Path(__file__).parent.parent.parent
            schema_path = project_root / "schemas" / "silver_layer_schema_v2.json"

        self.schema_path = Path(schema_path)

        if not self.schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {self.schema_path}")

        # Load schema for reference
        with open(self.schema_path, 'r') as f:
            self.schema = json.load(f)

    def validate(self, silver_data: dict) -> Dict[str, Any]:
        """
        Validate silver layer data

        Args:
            silver_data: Dictionary containing systems array

        Returns:
            Validation result dictionary with:
            - valid: bool
            - errors: list of error messages
            - warnings: list of warning messages
            - stats: statistics about the data
        """
        errors = []
        warnings = []

        # Check root structure
        if not isinstance(silver_data, dict):
            errors.append("Root must be a dictionary")
            return {
                "valid": False,
                "errors": errors,
                "warnings": warnings,
                "stats": {}
            }

        if "systems" not in silver_data:
            errors.append("Missing 'systems' key at root level")
            return {
                "valid": False,
                "errors": errors,
                "warnings": warnings,
                "stats": {}
            }

        systems = silver_data["systems"]

        if not isinstance(systems, list):
            errors.append("'systems' must be an array")
            return {
                "valid": False,
                "errors": errors,
                "warnings": warnings,
                "stats": {}
            }

        # Validate each system
        for i, system in enumerate(systems):
            system_errors, system_warnings = self._validate_system(system, i)
            errors.extend(system_errors)
            warnings.extend(system_warnings)

        # Collect stats
        stats = self._collect_stats(silver_data)

        # Log results
        if errors:
            logger.warning(f"Validation found {len(errors)} errors")
            for error in errors[:5]:  # Log first 5
                logger.warning(f"  - {error}")
            if len(errors) > 5:
                logger.warning(f"  ... and {len(errors) - 5} more errors")

        if warnings:
            logger.info(f"Validation found {len(warnings)} warnings")
            for warning in warnings[:5]:  # Log first 5
                logger.info(f"  - {warning}")

        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "stats": stats
        }

    def _validate_system(self, system: dict, index: int) -> tuple:
        """Validate a single system object"""
        errors = []
        warnings = []

        if not isinstance(system, dict):
            errors.append(f"System {index}: Must be a dictionary")
            return errors, warnings

        # Check required fields
        if "system_id" not in system:
            errors.append(f"System {index}: Missing required field 'system_id'")
        elif not system["system_id"]:
            errors.append(f"System {index}: 'system_id' cannot be empty")

        if "components" not in system:
            errors.append(f"System {index}: Missing required field 'components'")
        elif not isinstance(system["components"], list):
            errors.append(f"System {index}: 'components' must be an array")
        elif len(system["components"]) == 0:
            errors.append(f"System {index}: Must have at least one component")
        else:
            # Validate components
            for j, component in enumerate(system["components"]):
                comp_errors, comp_warnings = self._validate_component(component, index, j)
                errors.extend(comp_errors)
                warnings.extend(comp_warnings)

        if "metadata" not in system:
            warnings.append(f"System {index}: Missing 'metadata' field")

        # Validate system_attributes if present
        if "system_attributes" in system and system["system_attributes"] is not None:
            attrs = system["system_attributes"]

            if not isinstance(attrs, dict):
                errors.append(f"System {index}: 'system_attributes' must be a dictionary or null")
            else:
                # Check for source_sheet
                if "source_sheet" not in attrs:
                    warnings.append(f"System {index}: 'system_attributes' missing 'source_sheet'")

                # Check data types
                if "tonnage" in attrs and attrs["tonnage"] is not None:
                    if not isinstance(attrs["tonnage"], (int, float)):
                        errors.append(f"System {index}: 'tonnage' must be a number, got {type(attrs['tonnage'])}")

                if "capacity_btu" in attrs and attrs["capacity_btu"] is not None:
                    if not isinstance(attrs["capacity_btu"], int):
                        errors.append(f"System {index}: 'capacity_btu' must be an integer")

                if "total_price" in attrs and attrs["total_price"] is not None:
                    if not isinstance(attrs["total_price"], (int, float)):
                        errors.append(f"System {index}: 'total_price' must be a number")

                # Check system_type enum
                if "system_type" in attrs:
                    valid_types = ["AC", "HP", "Ductless", "MultiZone", "Package", "Unknown"]
                    if attrs["system_type"] not in valid_types:
                        warnings.append(f"System {index}: 'system_type' should be one of {valid_types}, got '{attrs['system_type']}'")

        return errors, warnings

    def _validate_component(self, component: dict, system_index: int, component_index: int) -> tuple:
        """Validate a single component"""
        errors = []
        warnings = []

        if not isinstance(component, dict):
            errors.append(f"System {system_index}, Component {component_index}: Must be a dictionary")
            return errors, warnings

        # Check required fields
        if "component_type" not in component:
            errors.append(f"System {system_index}, Component {component_index}: Missing 'component_type'")
        else:
            valid_types = ["ODU", "IDU", "Coil", "Furnace", "AirHandler", "AuxHeat", "Thermostat", "Accessory", "LineSet", "Other"]
            if component["component_type"] not in valid_types:
                warnings.append(f"System {system_index}, Component {component_index}: "
                              f"'component_type' should be one of {valid_types}, got '{component['component_type']}'")

        if "model_number" not in component:
            errors.append(f"System {system_index}, Component {component_index}: Missing 'model_number'")
        elif not component["model_number"] or component["model_number"] in ["", "N/A", "nan"]:
            errors.append(f"System {system_index}, Component {component_index}: Invalid 'model_number': {component['model_number']}")

        # Check price if present
        if "price" in component and component["price"] is not None:
            if not isinstance(component["price"], (int, float)):
                errors.append(f"System {system_index}, Component {component_index}: 'price' must be a number")
            elif component["price"] < 0:
                warnings.append(f"System {system_index}, Component {component_index}: Negative price: {component['price']}")

        return errors, warnings

    def _collect_stats(self, silver_data: dict) -> dict:
        """Collect statistics about the data"""
        systems = silver_data.get("systems", [])

        total_components = 0
        component_types = {}
        system_types = {}
        data_quality_counts = {"high": 0, "medium": 0, "low": 0}

        for system in systems:
            # Count components
            components = system.get("components", [])
            total_components += len(components)

            # Count component types
            for comp in components:
                comp_type = comp.get("component_type", "Unknown")
                component_types[comp_type] = component_types.get(comp_type, 0) + 1

            # Count system types
            if system.get("system_attributes"):
                sys_type = system["system_attributes"].get("system_type", "Unknown")
                system_types[sys_type] = system_types.get(sys_type, 0) + 1

            # Count data quality
            if system.get("metadata"):
                quality = system["metadata"].get("data_quality", "medium")
                if quality in data_quality_counts:
                    data_quality_counts[quality] += 1

        return {
            "total_systems": len(systems),
            "total_components": total_components,
            "avg_components_per_system": round(total_components / len(systems), 2) if len(systems) > 0 else 0,
            "component_types": component_types,
            "system_types": system_types,
            "data_quality": data_quality_counts
        }


def validate_silver(silver_data: dict, schema_path: str = None) -> Dict[str, Any]:
    """
    Convenience function for validation

    Args:
        silver_data: Silver layer data to validate
        schema_path: Optional path to schema file

    Returns:
        Validation result dictionary
    """
    validator = SilverValidator(schema_path)
    return validator.validate(silver_data)
