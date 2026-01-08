"""
CLI entry point for Stage 3 Loader

Usage:
    python -m src.stage3_loader <input_json> [output_xlsx] [costbook_title]

Examples:
    python -m src.stage3_loader data/silver/example.json
    python -m src.stage3_loader data/silver/example.json data/gold/output.xlsx
    python -m src.stage3_loader data/silver/example.json data/gold/output.xlsx "My Costbook"
"""
import sys
from .silver_to_excel_loader import main


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else None
    costbook_title = sys.argv[3] if len(sys.argv) > 3 else "WinSupply"

    try:
        main(input_path, output_path, costbook_title)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
