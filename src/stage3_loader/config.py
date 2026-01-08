"""
Configuration for Stage 3 Excel Loader
"""
import os

# Path to taxonomy configuration
TAXONOMY_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "config", "taxonomy.json")

# Excel column definitions (32 columns total)
EXCEL_COLUMNS = [
    "Costbook Title",
    "Job Name",
    "Job Description",
    "Item Name",
    "Item Description",
    "Item #/SKU",
    "Unit Cost",
    "Apply Tax",
    "Quantity",
    "Product or Service",
    "Pricebook Category 1",
    "Pricebook Category 2",
    "Pricebook Category 3",
    "Pricebook Category 4",
    "Pricebook Category 5",
    "Pricebook Category 6",
    "Pricebook Category 7",
    "Pricebook Category 8",
    "Pricebook Category 9",
    "Pricebook Category 10",
    "Custom Filter 1",  # Tonnage
    "Custom Filter 2",  # Capacity
    "Custom Filter 3",  # SEER 2
    "Custom Filter 4",  # EER2
    "Custom Filter 5",  # HSPF 2
    "Custom Filter 6",  # Fuel Source
    "Custom Filter 7",  # Compressor
    "Custom Filter 8",  # Reserved
    "Custom Filter 9",  # Reserved
    "Custom Filter 10", # Reserved
    "Custom Filter 11", # Reserved
    "Custom Filter 12", # Reserved
]

# Header row descriptions for columns (row 0 in Excel)
EXCEL_COLUMN_DESCRIPTIONS = [
    "This is the Costbook name. Every item within this importshould have this coumn filled will the correct costbook.",
    "This is the Flat Rate Job name. Each item with the same 'Job Name' will be a part of the same flat rate job.",
    "This is the description of the 'Flat Rate Job'. The first 'Job Description' for each flat rate job will be used.",
    "This is the 'Item Name' for the items within the flat rate job.",
    "This is the description of the Item",
    "This is the item #/SKU",
    "This is the 'Unit Cost' for the item. Leave blank for 'Labor & Overhead'",
    "This sets tax on the item.\n\n'Yes' or 'No'\n\nBlank defaults to 'No'",
    "This sets the quantity of the item. Blank defaults to 1.",
    "This sets whether the item is a 'Product' or 'Service.' Blank defaults to 'Product'",
    "This sets the category that the flat rate job will fall under.",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "Custom Filter 1",
    "Custom Filter 2",
    "Custom Filter 3",
    "Custom Filter 4",
    "Custom Filter 5",
    "Custom Filter 6",
    "Custom Filter 7",
    "Custom Filter 8",
    "Custom Filter 9",
    "Custom Filter 10",
    "Custom Filter 11",
    "Custom Filter 12",
]

# Custom filter definitions (taxonomy v2.0)
CUSTOM_FILTERS = [
    {"name": "Tonnage", "type": "Multi-Select"},
    {"name": "Capacity", "type": "Multi-Select"},
    {"name": "SEER 2", "type": "Multi-Select"},
    {"name": "EER 2", "type": "Multi-Select"},
    {"name": "HSPF 2", "type": "Multi-Select"},
    {"name": "Fuel Source", "type": "Multi-Select"},
    {"name": "Compressor", "type": "Multi-Select"},
]

# Filter sheet column names
FILTER_COLUMNS = [
    "Enter all filter types listed in Sheet 1, columns U to AC.",
    "Choose a type: Single Select or Multi-Select.",
]

# Filter sheet header row
FILTER_HEADER = [
    "Filter Name",
    "Filter Type",
]

# Default values for item fields
DEFAULT_APPLY_TAX = "Yes"
DEFAULT_QUANTITY = 1
DEFAULT_PRODUCT_OR_SERVICE = "Product"
DEFAULT_PRICEBOOK_CATEGORY = "-"

# Component type display names
COMPONENT_TYPE_DISPLAY = {
    "ODU": "AC",
    "Coil": "Evap Coil",
    "Furnace": "Furnace",
    "IDU": "Indoor Unit",
    "AHU": "Air Handler",
    "Outdoor Unit": "AC",
    "Evaporator": "Evap Coil",
    "Air Handler": "Air Handler",
}

# System type display names (v3.0 - granular packaged types)
SYSTEM_TYPE_DISPLAY = {
    "AC": "AC",
    "Heat Pump": "Heat Pump",
    "HP": "Heat Pump",
    "Mini Split": "Ductless",
    "Ductless": "Ductless",
    "Packaged": "Packaged AC",  # Legacy fallback
    "Package": "Packaged AC",   # Legacy fallback
}

# Stages display names (v3.0)
STAGES_DISPLAY = {
    "single": "Single Stage",
    "single_stage": "Single Stage",
    "two": "Two Stage",
    "two_stage": "Two Stage",
    "2": "Two Stage",
    "variable": "Variable Speed",
    "variable_speed": "Variable Speed",
    "inverter": "Variable Speed",
    "modulating": "Variable Speed",
    "multi": "Multi Stage",
}

# Orientation display names for item descriptions
ORIENTATION_DISPLAY = {
    "upflow": "Upflow",
    "downflow": "Downflow",
    "horizontal": "Horizontal",
    "multi": "Multi-Position",
    "multiposition": "Multi-Position",
    "multi-position": "Multi-Position",
}

# Efficiency rating labels for item descriptions
EFFICIENCY_LABELS = {
    "seer2": "SEER2",
    "seer": "SEER",
    "eer2": "EER2",
    "eer": "EER",
    "hspf2": "HSPF2",
    "hspf": "HSPF",
    "afue": "AFUE",
}

# Component description display names (more descriptive than COMPONENT_TYPE_DISPLAY)
COMPONENT_DESC_DISPLAY = {
    "ODU": "Condenser",
    "Coil": "Evaporator Coil",
    "Furnace": "Furnace",
    "IDU": "Indoor Unit",
    "AHU": "Air Handler",
    "AirHandler": "Air Handler",
    "Outdoor Unit": "Condenser",
    "Evaporator": "Evaporator Coil",
    "Air Handler": "Air Handler",
    "Thermostat": "Thermostat",
    "Accessory": "Accessory",
    "LineSet": "Line Set",
}
