"""
Pipeline orchestrator - coordinates the 3-stage ETL pipeline

Includes structured logging and LangWatch integration for observability.
"""
import json
import shutil
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Callable, Optional, Dict, Any, List
import logging
import sys

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from api.logging.structured_logger import JobLogger

# LangWatch integration with graceful fallback
try:
    from api.services.langwatch_service import (
        langwatch_trace,
        update_current_trace,
        get_langwatch_service,
        create_span,
        add_span_evaluation
    )
    _langwatch_available = True
except ImportError:
    _langwatch_available = False
    def langwatch_trace(name, metadata=None):
        def decorator(func):
            return func
        return decorator
    def update_current_trace(**kwargs): pass
    def get_langwatch_service(): return None
    from contextlib import contextmanager
    @contextmanager
    def create_span(name, span_type="span"): yield None
    def add_span_evaluation(span, name, passed, score=None, details=None): pass

# Evaluation service for silver layer quality assessment
try:
    from api.services.evaluation_service import (
        extract_bronze_identifiers,
        extract_silver_identifiers,
        run_silver_evaluations
    )
    _evaluation_available = True
except ImportError:
    _evaluation_available = False
    def extract_bronze_identifiers(data): return {}
    def extract_silver_identifiers(data): return {}
    def run_silver_evaluations(bronze, silver, span=None): return {}

# Lineage service
try:
    from api.services.lineage_service import LineageService
    from api.database.connection import get_db
    _lineage_available = True
except ImportError:
    _lineage_available = False

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Orchestrates the 3-stage ETL pipeline"""

    def __init__(
        self,
        job_id: str,
        jobs_base_dir: str = "./jobs",
        openrouter_api_key: str = None,
        llm_model: str = "anthropic/claude-sonnet-4",
        progress_callback: Optional[Callable[[str, int, str], None]] = None
    ):
        """
        Initialize the pipeline orchestrator.

        Args:
            job_id: Unique job identifier
            jobs_base_dir: Base directory for job artifacts
            openrouter_api_key: API key for OpenRouter
            llm_model: LLM model to use for transformation
            progress_callback: Callback function(stage, percent, message)
        """
        self.job_id = job_id
        self.job_dir = Path(jobs_base_dir) / job_id
        self.jobs_base_dir = jobs_base_dir
        self.api_key = openrouter_api_key
        self.llm_model = llm_model
        self.progress_callback = progress_callback

        # Create job directories
        self.input_dir = self.job_dir / "input"
        self.bronze_dir = self.job_dir / "bronze"
        self.silver_dir = self.job_dir / "silver"
        self.gold_dir = self.job_dir / "gold"

        for d in [self.input_dir, self.bronze_dir, self.silver_dir, self.gold_dir]:
            d.mkdir(parents=True, exist_ok=True)

        # Initialize structured logger
        self.job_logger = JobLogger(job_id, jobs_base_dir)
        self.job_logger.info("pipeline_init", f"Initialized pipeline for job {job_id}", {
            "llm_model": llm_model,
            "jobs_base_dir": jobs_base_dir
        })

        logger.info(f"Initialized PipelineOrchestrator for job {job_id}")

    def _update_progress(self, stage: str, percent: int, message: str):
        """Update job progress via callback"""
        if self.progress_callback:
            self.progress_callback(stage, percent, message)
        logger.info(f"[{self.job_id}] {stage}: {percent}% - {message}")

    @langwatch_trace(name="etl_pipeline")
    def run_pipeline(
        self,
        input_file: Path,
        costbook_title: str = "WinSupply",
        enable_ahri_enrichment: bool = False
    ) -> Dict[str, Any]:
        """
        Execute the full ETL pipeline.

        Args:
            input_file: Path to input Excel/PDF file
            costbook_title: Title for the costbook
            enable_ahri_enrichment: Whether to enable AHRI enrichment

        Returns:
            dict with results and statistics
        """
        results = {
            "job_id": self.job_id,
            "stages": {},
            "output_file": None,
            "stats": {}
        }

        # Update LangWatch trace with job metadata
        if _langwatch_available:
            update_current_trace(
                metadata={
                    "job_id": self.job_id,
                    "input_file": str(input_file),
                    "costbook_title": costbook_title,
                    "enable_ahri_enrichment": enable_ahri_enrichment
                },
                thread_id=self.job_id
            )

            # Store trace ID for lineage
            service = get_langwatch_service()
            if service:
                trace_id = service.get_current_trace_id()
                if trace_id:
                    self._record_trace_id(trace_id)

        try:
            # Detect file type
            suffix = input_file.suffix.lower()
            is_pdf = suffix == ".pdf"
            is_excel = suffix in [".xlsx", ".xls", ".xlsm", ".xlsb"]

            if not (is_pdf or is_excel):
                raise ValueError(f"Unsupported file type: {suffix}")

            source_type = "pdf" if is_pdf else "excel"

            # Log input file
            self.job_logger.record_input(
                str(input_file),
                file_hash=self._compute_file_hash(input_file),
                file_size=input_file.stat().st_size if input_file.exists() else None
            )
            self.job_logger.info("pipeline_start", f"Starting pipeline for {source_type} file", {
                "filename": input_file.name,
                "source_type": source_type,
                "costbook_title": costbook_title,
                "ahri_enrichment": enable_ahri_enrichment
            })

            logger.info(f"Processing {source_type} file: {input_file.name}")

            # Stage 1: Extraction (Bronze)
            self._update_progress("stage1", 0, "Starting extraction...")
            self.job_logger.stage_start("stage1_extraction", {"source_type": source_type})

            bronze_data = None  # Will hold loaded bronze data for evaluation
            bronze_ids = {}     # Will hold extracted identifiers

            with create_span("bronze_extraction", span_type="tool") as span:
                bronze_path = self._run_stage1(input_file, is_pdf)
                bronze_record_count = self._count_bronze_records(bronze_path)

                # Load bronze data and extract identifiers for evaluation
                bronze_data = self._load_bronze_data(bronze_path)
                if _evaluation_available and bronze_data:
                    bronze_ids = extract_bronze_identifiers(bronze_data)

                # Update span with extraction results including identifiers
                if span:
                    try:
                        span_output = {
                            "message": f"Extracted {bronze_record_count} records from {source_type}",
                            "record_count": bronze_record_count,
                            "source_type": source_type
                        }
                        # Add identifier summary to span output
                        if bronze_ids:
                            span_output["identifiers"] = {
                                "model_numbers_count": len(bronze_ids.get("model_numbers", [])),
                                "ahri_numbers_count": len(bronze_ids.get("ahri_numbers", [])),
                                "model_numbers_sample": bronze_ids.get("model_numbers", [])[:20],
                                "ahri_numbers_sample": bronze_ids.get("ahri_numbers", [])[:10]
                            }
                        # Add data preview (truncated)
                        if bronze_ids.get("records_preview"):
                            span_output["data_preview"] = bronze_ids["records_preview"][:10]

                        span.update(
                            output=span_output,
                            metadata={"source_type": source_type, "record_count": bronze_record_count}
                        )
                    except Exception as e:
                        logger.debug(f"Failed to update bronze span: {e}")

            self.job_logger.stage_end("stage1_extraction", {"output_path": str(bronze_path), "record_count": bronze_record_count})
            self.job_logger.record_output("bronze", str(bronze_path), bronze_record_count)
            results["stages"]["stage1"] = {
                "output": str(bronze_path),
                "source_type": source_type,
                "record_count": bronze_record_count
            }
            self._update_progress("stage1", 100, "Extraction complete")

            # Stage 2: Transformation (Silver) - includes LLM calls
            self._update_progress("stage2", 0, "Starting transformation...")
            self.job_logger.stage_start("stage2_transformation", {"enable_ahri": enable_ahri_enrichment})

            silver_data = None  # Will hold loaded silver data for evaluation
            evaluation_results = {}  # Will hold evaluation results

            with create_span("silver_transformation", span_type="chain") as span:
                silver_path, transform_stats = self._run_stage2(
                    bronze_path,
                    enable_ahri_enrichment
                )
                systems_count = transform_stats.get("total_systems", 0)

                # Load silver data for evaluation
                silver_data = self._load_silver_data(silver_path)

                # Run evaluations if both bronze and silver data available
                if _evaluation_available and bronze_data and silver_data:
                    try:
                        evaluation_results = run_silver_evaluations(
                            bronze_data,
                            silver_data,
                            span  # Attach evaluations to span
                        )
                        logger.info(f"Evaluations complete: {evaluation_results.get('summary', {})}")
                    except Exception as e:
                        logger.warning(f"Evaluation failed: {e}")

                # Update span with transformation results
                if span:
                    try:
                        # Build span input showing what we received from bronze
                        span_input = {
                            "bronze_record_count": bronze_record_count,
                            "bronze_model_numbers_count": len(bronze_ids.get("model_numbers", [])),
                            "bronze_ahri_count": len(bronze_ids.get("ahri_numbers", []))
                        }
                        if bronze_ids.get("records_preview"):
                            span_input["bronze_data_preview"] = bronze_ids["records_preview"][:5]

                        # Build span output showing what silver produced
                        silver_ids = evaluation_results.get("silver_identifiers", {})
                        span_output = {
                            "message": f"Transformed to {systems_count} systems",
                            "systems_count": systems_count,
                            "sources_processed": transform_stats.get("sources_processed", 0)
                        }
                        if silver_ids:
                            span_output["identifiers"] = {
                                "system_ids_count": len(silver_ids.get("system_ids", [])),
                                "model_numbers_count": len(silver_ids.get("model_numbers", [])),
                                "ahri_numbers_count": len(silver_ids.get("ahri_numbers", [])),
                                "system_ids_sample": silver_ids.get("system_ids", [])[:10],
                                "model_numbers_sample": silver_ids.get("model_numbers", [])[:20]
                            }
                        if silver_ids.get("systems_preview"):
                            span_output["data_preview"] = silver_ids["systems_preview"][:5]

                        # Add evaluation summary to metadata
                        eval_summary = evaluation_results.get("summary", {})
                        span.update(
                            input=span_input,
                            output=span_output,
                            metadata={
                                "systems_count": systems_count,
                                "sources_processed": transform_stats.get("sources_processed", 0),
                                "enable_ahri": enable_ahri_enrichment,
                                "evaluation_all_passed": eval_summary.get("all_passed", None),
                                "evaluation_avg_score": eval_summary.get("average_score", None)
                            }
                        )
                    except Exception as e:
                        logger.debug(f"Failed to update silver span: {e}")

            self.job_logger.stage_end("stage2_transformation", {"output_path": str(silver_path), "systems_count": systems_count})
            self.job_logger.record_output("silver", str(silver_path), systems_count)
            results["stages"]["stage2"] = {
                "output": str(silver_path),
                "stats": transform_stats,
                "evaluations": evaluation_results.get("evaluations", {}) if evaluation_results else {}
            }
            self._update_progress("stage2", 100, "Transformation complete")

            # Stage 3: Loading (Gold)
            self._update_progress("stage3", 0, "Generating Excel output...")
            self.job_logger.stage_start("stage3_loading", {"costbook_title": costbook_title})

            with create_span("gold_output", span_type="tool") as span:
                gold_path = self._run_stage3(silver_path, costbook_title, input_file.stem)
                gold_row_count = self._count_gold_rows(gold_path)

                # Update span with loading results
                if span:
                    try:
                        span.update(
                            output=f"Generated Excel with {gold_row_count} rows",
                            metadata={
                                "row_count": gold_row_count,
                                "costbook_title": costbook_title,
                                "output_file": gold_path.name
                            }
                        )
                    except Exception as e:
                        logger.debug(f"Failed to update gold span: {e}")

            self.job_logger.stage_end("stage3_loading", {"output_path": str(gold_path), "row_count": gold_row_count})
            self.job_logger.record_output("gold", str(gold_path), gold_row_count)
            results["stages"]["stage3"] = {
                "output": str(gold_path),
                "row_count": gold_row_count
            }
            self._update_progress("stage3", 100, "Loading complete")

            results["output_file"] = gold_path.name
            results["stats"] = {
                "source_type": source_type,
                "systems_count": transform_stats.get("total_systems", 0),
                "sources_processed": transform_stats.get("sources_processed", 0)
            }

            # Add evaluation summary if available
            if evaluation_results:
                eval_summary = evaluation_results.get("summary", {})
                results["evaluation_summary"] = {
                    "all_passed": eval_summary.get("all_passed", None),
                    "average_score": eval_summary.get("average_score", None),
                    "completeness_score": evaluation_results.get("evaluations", {}).get("completeness", {}).get("score"),
                    "schema_valid": evaluation_results.get("evaluations", {}).get("schema_valid", {}).get("passed"),
                    "consistency_score": evaluation_results.get("evaluations", {}).get("field_consistency", {}).get("score"),
                    "ahri_valid": evaluation_results.get("evaluations", {}).get("ahri_validation", {}).get("passed")
                }

            # Finalize logs
            self.job_logger.finalize(success=True)

            logger.info(f"Pipeline completed for job {self.job_id}: {results['stats']}")
            return results

        except Exception as e:
            self.job_logger.error("pipeline_error", f"Pipeline failed: {str(e)}", {"error_type": type(e).__name__})
            self.job_logger.finalize(success=False, error_message=str(e))
            logger.exception(f"Pipeline failed for job {self.job_id}")
            raise

    def _compute_file_hash(self, file_path: Path) -> Optional[str]:
        """Compute SHA256 hash of a file"""
        try:
            with open(file_path, 'rb') as f:
                return hashlib.sha256(f.read()).hexdigest()
        except:
            return None

    def _count_bronze_records(self, bronze_path: Path) -> int:
        """Count records in bronze JSON"""
        try:
            with open(bronze_path, 'r') as f:
                data = json.load(f)
                if isinstance(data, list):
                    return len(data)
                elif isinstance(data, dict):
                    if 'tables' in data:
                        return sum(len(t.get('cells', [])) for t in data['tables'])
                    return len(data.get('records', data.get('data', [])))
        except:
            pass
        return 0

    def _load_bronze_data(self, bronze_path: Path) -> Optional[Any]:
        """Load bronze JSON data for evaluation"""
        try:
            with open(bronze_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.debug(f"Failed to load bronze data: {e}")
            return None

    def _load_silver_data(self, silver_path: Path) -> Optional[Dict[str, Any]]:
        """Load silver JSON data for evaluation"""
        try:
            with open(silver_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.debug(f"Failed to load silver data: {e}")
            return None

    def _count_gold_rows(self, gold_path: Path) -> int:
        """Count rows in gold Excel file"""
        try:
            import openpyxl
            wb = openpyxl.load_workbook(gold_path, read_only=True)
            ws = wb.active
            return ws.max_row - 1  # Exclude header
        except:
            pass
        return 0

    def _record_trace_id(self, trace_id: str) -> None:
        """Record LangWatch trace ID in lineage"""
        if _lineage_available:
            try:
                with get_db() as db:
                    lineage_service = LineageService(db)
                    lineage_service.set_langwatch_trace(self.job_id, trace_id)
            except Exception as e:
                logger.debug(f"Could not record trace ID: {e}")

    def _run_stage1(self, input_file: Path, is_pdf: bool) -> Path:
        """Run Stage 1: Extraction"""
        if is_pdf:
            from src.stage1_extractor.pdf_extractor import PDFExtractor
            extractor = PDFExtractor(str(input_file))
        else:
            from src.stage1_extractor.excel_extractor import ExcelExtractor
            extractor = ExcelExtractor(str(input_file))

        output_path = extractor.extract_to_json(str(self.bronze_dir))
        logger.info(f"Stage 1 output: {output_path}")
        return Path(output_path)

    def _run_stage2(self, bronze_path: Path, enable_ahri: bool) -> tuple:
        """Run Stage 2: Transformation"""
        from src.stage2_architect.bronze_json_transformer import BronzeJSONTransformer

        if not self.api_key:
            raise ValueError("OPENROUTER_API_KEY is required for Stage 2 transformation")

        transformer = BronzeJSONTransformer(
            api_key=self.api_key,
            model=self.llm_model,
            enable_ahri_enrichment=enable_ahri,
            job_id=self.job_id,
            job_logger=self.job_logger
        )

        result = transformer.transform(
            str(bronze_path),
            str(self.silver_dir)
        )

        # Extract silver path from result
        silver_path = Path(result.get("silver_path", ""))
        if not silver_path.exists():
            # Fallback: find the silver JSON in the output directory
            silver_files = list(self.silver_dir.glob("*.json"))
            if silver_files:
                silver_path = silver_files[0]
            else:
                raise ValueError("No silver JSON output found")

        stats = {
            "total_systems": result.get("total_systems", 0),
            "sources_processed": result.get("sources_processed", 0),
            "validation": result.get("validation", {})
        }

        logger.info(f"Stage 2 output: {silver_path}, stats: {stats}")
        return silver_path, stats

    def _run_stage3(self, silver_path: Path, costbook_title: str, base_name: str) -> Path:
        """Run Stage 3: Loading"""
        from src.stage3_loader.silver_to_excel_loader import SilverToExcelLoader

        loader = SilverToExcelLoader(costbook_title=costbook_title)

        # Generate output filename
        output_filename = f"{base_name}_costbook.xlsx"
        output_path = self.gold_dir / output_filename

        loader.convert(str(silver_path), str(output_path))
        logger.info(f"Stage 3 output: {output_path}")
        return output_path

    def cleanup(self):
        """Remove job directory and all artifacts"""
        if self.job_dir.exists():
            shutil.rmtree(self.job_dir)
            logger.info(f"Cleaned up job directory: {self.job_dir}")
