"""
Validates generated transformer by running its self-tests
"""

import subprocess
import logging
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger(__name__)


def validate_transformer(transformer_path: str) -> Dict[str, Any]:
    """
    Run transformer's self-tests to validate it works

    Args:
        transformer_path: Path to generated transformer .py file

    Returns:
        Dict with 'passed' (bool), 'output' (str), 'error' (str)
    """
    logger.info(f"Validating transformer: {transformer_path}")

    try:
        # Run the transformer (which runs __main__ block with tests)
        result = subprocess.run(
            ['python3', transformer_path],
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode == 0:
            logger.info("✅ Transformer self-tests passed")
            return {
                "passed": True,
                "output": result.stdout,
                "error": None
            }
        else:
            logger.error(f"❌ Transformer self-tests failed:\n{result.stderr}")
            return {
                "passed": False,
                "output": result.stdout,
                "error": result.stderr
            }

    except subprocess.TimeoutExpired:
        logger.error("❌ Transformer validation timed out")
        return {
            "passed": False,
            "output": "",
            "error": "Validation timeout (30s)"
        }
    except Exception as e:
        logger.error(f"❌ Validation error: {e}")
        return {
            "passed": False,
            "output": "",
            "error": str(e)
        }
