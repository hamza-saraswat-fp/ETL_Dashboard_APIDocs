"""
Module 2: The Architect
Generates custom transformer code using LLM
"""

import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

from .sampler import CSVSampler
from .llm_client import LLMClient
from .validator import validate_transformer

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Architect:
    """Generates transformer code for a specific supplier catalog"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.sampler = CSVSampler(rows_per_section=20)
        self.llm_client = LLMClient(api_key)

        # Load prompt template
        self.prompt_template = self._load_prompt_template()

    def _load_prompt_template(self) -> str:
        """Load prompt template from config"""
        project_root = Path(__file__).parent.parent.parent
        prompt_path = project_root / "config" / "prompts" / "transformer_prompt.txt"

        with open(prompt_path, 'r') as f:
            return f.read()

    def generate_transformer(self, bronze_csv_path: str) -> Dict[str, Any]:
        """
        Generate transformer for a bronze layer CSV

        Args:
            bronze_csv_path: Path to bronze layer CSV from Module 1

        Returns:
            Dict with transformer_path, metadata, validation_result
        """
        logger.info(f"=== Generating Transformer ===")
        logger.info(f"Input: {bronze_csv_path}")

        # Step 1: Sample CSV
        logger.info("Step 1: Sampling CSV data")
        sampled_df, sampling_metadata = self.sampler.sample(bronze_csv_path)
        csv_sample = sampled_df.to_csv(index=False)

        logger.info(f"Sample size: {len(csv_sample)} characters")

        # Step 2: Build prompt
        logger.info("Step 2: Building LLM prompt")
        prompt = self.prompt_template.format(csv_sample=csv_sample)

        # Step 3: Call LLM
        logger.info("Step 3: Calling LLM to generate transformer code")
        generated_code = self.llm_client.generate_transformer(prompt)

        # Step 4: Save transformer
        logger.info("Step 4: Saving generated transformer")
        transformer_path = self._save_transformer(bronze_csv_path, generated_code)

        # Step 5: Validate (run self-tests)
        logger.info("Step 5: Validating transformer")
        validation_result = validate_transformer(transformer_path)

        if not validation_result['passed']:
            logger.error(f"❌ Transformer validation failed!")
            logger.error(validation_result['error'])
            raise Exception(f"Generated transformer failed validation: {validation_result['error']}")

        logger.info(f"✅ Transformer generated and validated: {transformer_path}")

        return {
            "transformer_path": transformer_path,
            "sampling_metadata": sampling_metadata,
            "validation_result": validation_result
        }

    def _save_transformer(self, bronze_csv_path: str, code: str) -> str:
        """Save generated transformer code"""
        project_root = Path(__file__).parent.parent.parent
        transformers_dir = project_root / "transformers"
        transformers_dir.mkdir(exist_ok=True)

        # Generate filename: supplername_timestamp.py
        csv_filename = Path(bronze_csv_path).stem
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        transformer_filename = f"{csv_filename}_{timestamp}.py"
        transformer_path = transformers_dir / transformer_filename

        with open(transformer_path, 'w') as f:
            f.write(code)

        logger.info(f"Saved transformer: {transformer_path}")

        return str(transformer_path)


def main():
    """CLI entry point for Module 2"""
    import sys
    import os
    from pathlib import Path

    # Try to load API key from .env file
    env_path = Path(__file__).parent.parent.parent / '.env'
    api_key = None

    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                if line.startswith('OPENROUTER_API_KEY='):
                    api_key = line.strip().split('=', 1)[1]
                    break

    # Check command line arguments
    if len(sys.argv) < 2:
        print("Usage: python -m src.stage2_architect.architect <bronze_csv_path> [api_key]")
        print("Example: python -m src.stage2_architect.architect data/bronze/bayou_south.csv")
        print("\nAPI key can be provided via:")
        print("  1. Command line argument")
        print("  2. .env file (OPENROUTER_API_KEY=...)")
        sys.exit(1)

    bronze_csv_path = sys.argv[1]

    # Override with command line API key if provided
    if len(sys.argv) >= 3:
        api_key = sys.argv[2]

    if not api_key:
        print("Error: No API key found. Please provide via command line or .env file.")
        sys.exit(1)

    try:
        architect = Architect(api_key)
        result = architect.generate_transformer(bronze_csv_path)

        print("\n=== Transformer Generation Complete ===")
        print(f"Transformer: {result['transformer_path']}")
        print(f"Validation: {'✅ PASSED' if result['validation_result']['passed'] else '❌ FAILED'}")
        print(f"\nSampling Info:")
        print(f"  Total rows: {result['sampling_metadata']['total_rows']}")
        print(f"  Sampled rows: {result['sampling_metadata']['sampled_rows']}")
        print(f"  Sheets: {len(result['sampling_metadata']['sheets'])}")

    except FileNotFoundError as e:
        print(f"\nError: {e}")
        print("Please check the file path and try again.")
        sys.exit(1)

    except Exception as e:
        logger.error(f"Error generating transformer: {e}", exc_info=True)
        print(f"\nError: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
