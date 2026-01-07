"""
Evaluation Service for Silver Layer Quality Assessment

Provides functions to evaluate the quality of silver layer transformations
by comparing against bronze data and validating outputs.
"""
import json
import logging
import re
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)

# Column names that typically contain model numbers in bronze data
MODEL_NUMBER_COLUMNS = [
    "ODU", "Evap", "Furnace", "Fan Coil", "AUX", "Indoor", "Outdoor",
    "Coil", "AirHandler", "Air Handler", "IDU", "Thermostat", "LineSet",
    "Model", "Model Number", "model_number", "Part", "SKU"
]

# Column names that contain AHRI reference numbers
AHRI_COLUMNS = ["AHRI Ref", "AHRI Ref.", "ahri_number", "AHRI", "ahri"]


def extract_bronze_identifiers(bronze_data: Any) -> Dict[str, Any]:
    """
    Extract all identifiable values from bronze layer data.

    Scans bronze records for model numbers, AHRI references, and other
    identifiers that can be used to track systems through transformation.

    Args:
        bronze_data: Bronze layer data (list of records or dict with tables)

    Returns:
        Dictionary with:
        - ahri_numbers: List of AHRI reference numbers found
        - model_numbers: List of all model numbers found
        - record_count: Number of bronze records
        - records_preview: First 50 records (truncated)
    """
    ahri_numbers: Set[str] = set()
    model_numbers: Set[str] = set()
    records = []

    # Handle different bronze data formats
    if isinstance(bronze_data, list):
        records = bronze_data
    elif isinstance(bronze_data, dict):
        if "tables" in bronze_data:
            # Docling format - extract from cells
            for table in bronze_data.get("tables", []):
                cells = table.get("cells", [])
                # For cell-based data, we can't easily extract model numbers
                # Just note the table count
                pass
            return {
                "ahri_numbers": [],
                "model_numbers": [],
                "record_count": len(bronze_data.get("tables", [])),
                "records_preview": bronze_data.get("tables", [])[:50],
                "format": "docling_cells"
            }
        elif "records" in bronze_data:
            records = bronze_data["records"]
        elif "data" in bronze_data:
            records = bronze_data["data"]
        else:
            # Might be a single-table dict, check for list values
            for key, value in bronze_data.items():
                if isinstance(value, list) and len(value) > 0:
                    records = value
                    break

    # Extract identifiers from each record
    for record in records:
        if not isinstance(record, dict):
            continue

        # Extract AHRI numbers
        for col in AHRI_COLUMNS:
            value = record.get(col)
            if value and str(value).strip() and str(value).lower() not in ["none", "null", "nan", "n/a"]:
                ahri_str = str(value).strip()
                # Clean up AHRI - remove non-numeric characters
                ahri_clean = re.sub(r'[^\d]', '', ahri_str)
                if ahri_clean and len(ahri_clean) >= 5:
                    ahri_numbers.add(ahri_clean)

        # Extract model numbers from known columns
        for col in MODEL_NUMBER_COLUMNS:
            value = record.get(col)
            if value and str(value).strip() and str(value).lower() not in ["none", "null", "nan", "n/a", ""]:
                model_str = str(value).strip()
                # Clean model number - remove leading asterisks
                model_clean = model_str.lstrip("*").strip()
                if model_clean and len(model_clean) >= 3:
                    model_numbers.add(model_clean)

        # Also scan all columns for values that look like model numbers
        for key, value in record.items():
            if value and isinstance(value, str) and len(value) >= 5:
                value_str = value.strip()
                # Model numbers typically have letters and numbers
                if re.match(r'^[A-Z0-9*-]+$', value_str, re.IGNORECASE):
                    if any(c.isalpha() for c in value_str) and any(c.isdigit() for c in value_str):
                        model_clean = value_str.lstrip("*").strip()
                        if len(model_clean) >= 5:
                            model_numbers.add(model_clean)

    return {
        "ahri_numbers": sorted(list(ahri_numbers)),
        "model_numbers": sorted(list(model_numbers)),
        "record_count": len(records),
        "records_preview": records[:50],
        "format": "flat_records"
    }


def extract_silver_identifiers(silver_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract identifiers from silver layer data.

    Args:
        silver_data: Silver layer data with systems array

    Returns:
        Dictionary with:
        - system_ids: List of system IDs
        - model_numbers: List of all component model numbers
        - ahri_numbers: List of AHRI numbers from system attributes
        - systems_count: Number of systems
        - systems_preview: First 50 systems (truncated)
    """
    system_ids: Set[str] = set()
    model_numbers: Set[str] = set()
    ahri_numbers: Set[str] = set()

    systems = silver_data.get("systems", [])

    for system in systems:
        if not isinstance(system, dict):
            continue

        # Extract system_id
        sys_id = system.get("system_id")
        if sys_id and str(sys_id).strip():
            system_ids.add(str(sys_id).strip())

        # Extract AHRI from system_attributes
        attrs = system.get("system_attributes", {})
        if attrs:
            ahri = attrs.get("ahri_number")
            if ahri and str(ahri).strip() and str(ahri).lower() not in ["none", "null", "nan"]:
                ahri_clean = re.sub(r'[^\d]', '', str(ahri))
                if ahri_clean and len(ahri_clean) >= 5:
                    ahri_numbers.add(ahri_clean)

        # Extract model numbers from components
        components = system.get("components", [])
        for comp in components:
            if not isinstance(comp, dict):
                continue
            model = comp.get("model_number")
            if model and str(model).strip() and str(model).lower() not in ["none", "null", "nan", "n/a"]:
                model_clean = str(model).strip().lstrip("*")
                if len(model_clean) >= 3:
                    model_numbers.add(model_clean)

    return {
        "system_ids": sorted(list(system_ids)),
        "model_numbers": sorted(list(model_numbers)),
        "ahri_numbers": sorted(list(ahri_numbers)),
        "systems_count": len(systems),
        "systems_preview": systems[:50]
    }


def evaluate_completeness(
    bronze_ids: Dict[str, Any],
    silver_ids: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Evaluate completeness of silver layer transformation.

    Checks that all bronze model numbers appear in silver output.

    Args:
        bronze_ids: Result from extract_bronze_identifiers()
        silver_ids: Result from extract_silver_identifiers()

    Returns:
        Evaluation result with passed, score, and details
    """
    bronze_models = set(bronze_ids.get("model_numbers", []))
    silver_models = set(silver_ids.get("model_numbers", []))

    if not bronze_models:
        # No bronze model numbers to compare
        return {
            "passed": True,
            "score": 1.0,
            "details": json.dumps({
                "bronze_count": 0,
                "silver_count": len(silver_models),
                "message": "No bronze model numbers to compare",
                "match_rate": "N/A"
            })
        }

    missing = bronze_models - silver_models
    extra = silver_models - bronze_models
    found = bronze_models & silver_models

    score = len(found) / len(bronze_models)

    return {
        "passed": len(missing) == 0,
        "score": round(score, 4),
        "details": json.dumps({
            "bronze_count": len(bronze_models),
            "silver_count": len(silver_models),
            "found_count": len(found),
            "missing_count": len(missing),
            "extra_count": len(extra),
            "missing": sorted(list(missing))[:20],
            "extra": sorted(list(extra))[:20],
            "match_rate": f"{score * 100:.1f}%"
        })
    }


def evaluate_schema(silver_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Evaluate silver layer schema validity.

    Uses SilverValidator to check schema compliance.

    Args:
        silver_data: Silver layer data with systems array

    Returns:
        Evaluation result with passed, score, and details
    """
    try:
        from src.stage2_architect.silver_validator import SilverValidator
        validator = SilverValidator()
        result = validator.validate(silver_data)

        errors = result.get("errors", [])
        warnings = result.get("warnings", [])
        stats = result.get("stats", {})

        # Calculate score based on error rate
        total_systems = stats.get("total_systems", 0)
        if total_systems > 0:
            # Assume each error affects one system
            error_rate = min(len(errors) / total_systems, 1.0)
            score = 1.0 - error_rate
        else:
            score = 1.0 if not errors else 0.0

        return {
            "passed": len(errors) == 0,
            "score": round(score, 4),
            "details": json.dumps({
                "valid": result.get("valid", False),
                "error_count": len(errors),
                "warning_count": len(warnings),
                "errors": errors[:20],
                "warnings": warnings[:10],
                "stats": stats
            })
        }

    except Exception as e:
        logger.warning(f"Schema validation failed: {e}")
        return {
            "passed": False,
            "score": 0.0,
            "details": json.dumps({
                "error": str(e),
                "message": "Schema validation encountered an error"
            })
        }


def evaluate_field_consistency(
    bronze_data: Any,
    silver_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Evaluate field consistency to detect potential hallucinations.

    Checks that silver numeric values can be traced back to bronze data.

    Args:
        bronze_data: Bronze layer data
        silver_data: Silver layer data

    Returns:
        Evaluation result with passed, score, and details
    """
    # Build set of all bronze values (normalized)
    bronze_values: Set[str] = set()

    # Extract bronze records
    records = []
    if isinstance(bronze_data, list):
        records = bronze_data
    elif isinstance(bronze_data, dict):
        if "tables" in bronze_data:
            # Can't easily compare cell-based data
            return {
                "passed": True,
                "score": 1.0,
                "details": json.dumps({
                    "message": "Skipped for cell-based bronze data",
                    "checked_count": 0
                })
            }
        records = bronze_data.get("records", bronze_data.get("data", []))

    # Collect all bronze values
    for record in records:
        if not isinstance(record, dict):
            continue
        for key, value in record.items():
            if value is not None:
                bronze_values.add(str(value).lower().strip())
                # Also add numeric variations
                try:
                    num = float(str(value).replace(",", "").replace("$", ""))
                    bronze_values.add(str(num))
                    bronze_values.add(str(int(num)))
                except (ValueError, TypeError):
                    pass

    # Check silver numeric fields against bronze
    hallucination_prone_fields = [
        "tonnage", "seer", "seer2", "eer", "eer2",
        "hspf", "hspf2", "capacity_btu", "total_price"
    ]

    suspicious_values = []
    checked_count = 0

    systems = silver_data.get("systems", [])
    for system in systems:
        attrs = system.get("system_attributes", {})
        if not attrs:
            continue

        for field in hallucination_prone_fields:
            value = attrs.get(field)
            if value is None:
                continue

            checked_count += 1
            value_str = str(value).lower().strip()

            # Check if value exists in bronze
            found = False
            if value_str in bronze_values:
                found = True
            else:
                # Try numeric comparison
                try:
                    num = float(value)
                    if str(num) in bronze_values or str(int(num)) in bronze_values:
                        found = True
                    # Also check with tolerance
                    for bv in bronze_values:
                        try:
                            if abs(float(bv) - num) < 0.1:
                                found = True
                                break
                        except (ValueError, TypeError):
                            continue
                except (ValueError, TypeError):
                    pass

            if not found:
                suspicious_values.append({
                    "system_id": system.get("system_id", "unknown"),
                    "field": field,
                    "value": value
                })

    # Calculate score
    if checked_count > 0:
        score = 1.0 - (len(suspicious_values) / checked_count)
        score = max(0.0, score)
    else:
        score = 1.0

    return {
        "passed": len(suspicious_values) == 0,
        "score": round(score, 4),
        "details": json.dumps({
            "checked_count": checked_count,
            "suspicious_count": len(suspicious_values),
            "suspicious_values": suspicious_values[:20],
            "bronze_values_sampled": len(bronze_values)
        })
    }


def evaluate_ahri_numbers(silver_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate AHRI numbers in silver data.

    AHRI numbers should be 7-10 digit numbers.

    Args:
        silver_data: Silver layer data

    Returns:
        Evaluation result with passed, score, and details
    """
    valid_count = 0
    invalid_count = 0
    missing_count = 0
    issues = []

    systems = silver_data.get("systems", [])

    for system in systems:
        attrs = system.get("system_attributes", {})
        if not attrs:
            # Standalone component, skip AHRI check
            continue

        ahri = attrs.get("ahri_number")

        if ahri is None or str(ahri).lower() in ["none", "null", "nan", ""]:
            missing_count += 1
            continue

        ahri_str = str(ahri).strip()
        # Remove any non-digit characters for validation
        ahri_digits = re.sub(r'[^\d]', '', ahri_str)

        # AHRI numbers should be 7-10 digits
        if ahri_digits.isdigit() and 7 <= len(ahri_digits) <= 10:
            valid_count += 1
        else:
            invalid_count += 1
            issues.append({
                "system_id": system.get("system_id", "unknown"),
                "ahri_value": ahri,
                "issue": f"Invalid format: expected 7-10 digits, got '{ahri_str}'"
            })

    total = valid_count + invalid_count + missing_count

    if total == 0:
        score = 1.0
    elif valid_count + missing_count == total:
        # No invalid formats (missing is acceptable)
        score = 1.0
    else:
        # Score based on valid out of those that have AHRI
        total_with_ahri = valid_count + invalid_count
        score = valid_count / total_with_ahri if total_with_ahri > 0 else 1.0

    return {
        "passed": invalid_count == 0,
        "score": round(score, 4),
        "details": json.dumps({
            "valid_count": valid_count,
            "invalid_count": invalid_count,
            "missing_count": missing_count,
            "total_systems": len(systems),
            "issues": issues[:20]
        })
    }


def run_silver_evaluations(
    bronze_data: Any,
    silver_data: Dict[str, Any],
    span: Optional[Any] = None
) -> Dict[str, Any]:
    """
    Run all silver layer evaluations and optionally attach to span.

    Args:
        bronze_data: Bronze layer data
        silver_data: Silver layer data
        span: Optional LangWatch span to attach evaluations to

    Returns:
        Dictionary with all evaluation results
    """
    from api.services.langwatch_service import add_span_evaluation

    # Extract identifiers
    bronze_ids = extract_bronze_identifiers(bronze_data)
    silver_ids = extract_silver_identifiers(silver_data)

    # Run all evaluations
    completeness = evaluate_completeness(bronze_ids, silver_ids)
    schema = evaluate_schema(silver_data)
    consistency = evaluate_field_consistency(bronze_data, silver_data)
    ahri = evaluate_ahri_numbers(silver_data)

    # Attach to span if provided
    if span:
        add_span_evaluation(span, "completeness", completeness["passed"], completeness["score"], completeness["details"])
        add_span_evaluation(span, "schema_valid", schema["passed"], schema["score"], schema["details"])
        add_span_evaluation(span, "field_consistency", consistency["passed"], consistency["score"], consistency["details"])
        add_span_evaluation(span, "ahri_validation", ahri["passed"], ahri["score"], ahri["details"])

    # Log summary
    logger.info(
        f"Silver evaluations: completeness={completeness['score']:.2f}, "
        f"schema={schema['score']:.2f}, consistency={consistency['score']:.2f}, "
        f"ahri={ahri['score']:.2f}"
    )

    return {
        "bronze_identifiers": bronze_ids,
        "silver_identifiers": silver_ids,
        "evaluations": {
            "completeness": completeness,
            "schema_valid": schema,
            "field_consistency": consistency,
            "ahri_validation": ahri
        },
        "summary": {
            "all_passed": all([
                completeness["passed"],
                schema["passed"],
                consistency["passed"],
                ahri["passed"]
            ]),
            "average_score": round(
                (completeness["score"] + schema["score"] +
                 consistency["score"] + ahri["score"]) / 4,
                4
            )
        }
    }
