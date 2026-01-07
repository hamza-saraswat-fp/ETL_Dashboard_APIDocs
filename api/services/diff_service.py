"""
JSON Diff Service for comparing pipeline outputs.

Provides side-by-side comparison of Bronze/Silver/Gold outputs
between different runs or within the same run.
"""
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum

logger = logging.getLogger(__name__)


class DiffType(str, Enum):
    """Type of difference"""
    ADDED = "added"
    REMOVED = "removed"
    MODIFIED = "modified"
    UNCHANGED = "unchanged"


class DiffService:
    """
    Service for computing JSON diffs between pipeline outputs.
    """

    def __init__(self, jobs_dir: str = "./jobs"):
        """
        Initialize diff service.

        Args:
            jobs_dir: Base directory for job files
        """
        self.jobs_dir = Path(jobs_dir)

    def get_stage_output(self, job_id: str, stage: str) -> Optional[Dict[str, Any]]:
        """
        Load stage output for a job.

        Args:
            job_id: Job ID
            stage: Stage name (bronze, silver, gold)

        Returns:
            Parsed JSON content or None
        """
        # Try to find the output file
        job_dir = self.jobs_dir / job_id
        if not job_dir.exists():
            return None

        # Stage-specific paths
        stage_paths = {
            "bronze": [job_dir / "bronze" / f"{job_id}_bronze.json", job_dir / "bronze.json"],
            "silver": [job_dir / "silver" / f"{job_id}_silver.json", job_dir / "silver.json"],
            "gold": [job_dir / "gold" / f"{job_id}_gold.xlsx"],  # Gold is Excel, not JSON
        }

        paths = stage_paths.get(stage, [])
        for path in paths:
            if path.exists() and path.suffix == ".json":
                try:
                    with open(path, 'r') as f:
                        return json.load(f)
                except Exception as e:
                    logger.error(f"Failed to load {path}: {e}")
                    return None

        # Also check by pattern
        for pattern in [f"*{stage}*.json", f"{stage}*.json"]:
            matches = list(job_dir.rglob(pattern))
            if matches:
                try:
                    with open(matches[0], 'r') as f:
                        return json.load(f)
                except Exception as e:
                    logger.error(f"Failed to load {matches[0]}: {e}")

        return None

    def compute_diff(
        self,
        json1: Dict[str, Any],
        json2: Dict[str, Any],
        path: str = ""
    ) -> List[Dict[str, Any]]:
        """
        Compute structured diff between two JSON objects.

        Args:
            json1: First JSON object
            json2: Second JSON object
            path: Current path for nested objects

        Returns:
            List of diff entries
        """
        diffs = []

        # Handle different types
        if type(json1) != type(json2):
            diffs.append({
                "path": path or "(root)",
                "type": DiffType.MODIFIED.value,
                "old_value": json1,
                "new_value": json2,
            })
            return diffs

        # Handle dicts
        if isinstance(json1, dict):
            all_keys = set(json1.keys()) | set(json2.keys())
            for key in sorted(all_keys):
                new_path = f"{path}.{key}" if path else key

                if key not in json1:
                    diffs.append({
                        "path": new_path,
                        "type": DiffType.ADDED.value,
                        "new_value": json2[key],
                    })
                elif key not in json2:
                    diffs.append({
                        "path": new_path,
                        "type": DiffType.REMOVED.value,
                        "old_value": json1[key],
                    })
                else:
                    diffs.extend(self.compute_diff(json1[key], json2[key], new_path))

        # Handle lists
        elif isinstance(json1, list):
            max_len = max(len(json1), len(json2))
            for i in range(max_len):
                new_path = f"{path}[{i}]"

                if i >= len(json1):
                    diffs.append({
                        "path": new_path,
                        "type": DiffType.ADDED.value,
                        "new_value": json2[i],
                    })
                elif i >= len(json2):
                    diffs.append({
                        "path": new_path,
                        "type": DiffType.REMOVED.value,
                        "old_value": json1[i],
                    })
                else:
                    diffs.extend(self.compute_diff(json1[i], json2[i], new_path))

        # Handle primitives
        else:
            if json1 != json2:
                diffs.append({
                    "path": path or "(root)",
                    "type": DiffType.MODIFIED.value,
                    "old_value": json1,
                    "new_value": json2,
                })

        return diffs

    def summarize_diff(self, diffs: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Summarize diff statistics.

        Args:
            diffs: List of diff entries

        Returns:
            Dict with counts by diff type
        """
        summary = {
            "added": 0,
            "removed": 0,
            "modified": 0,
            "total": len(diffs),
        }
        for diff in diffs:
            diff_type = diff.get("type", "")
            if diff_type in summary:
                summary[diff_type] += 1
        return summary

    def compare_jobs(
        self,
        job1_id: str,
        job2_id: str,
        stage: str
    ) -> Dict[str, Any]:
        """
        Compare outputs of two jobs at a specific stage.

        Args:
            job1_id: First job ID
            job2_id: Second job ID
            stage: Stage to compare (bronze, silver)

        Returns:
            Comparison result with diff and summary
        """
        json1 = self.get_stage_output(job1_id, stage)
        json2 = self.get_stage_output(job2_id, stage)

        if json1 is None:
            return {"error": f"Could not load {stage} output for job {job1_id}"}
        if json2 is None:
            return {"error": f"Could not load {stage} output for job {job2_id}"}

        diffs = self.compute_diff(json1, json2)

        return {
            "job1_id": job1_id,
            "job2_id": job2_id,
            "stage": stage,
            "json1": json1,
            "json2": json2,
            "diffs": diffs,
            "summary": self.summarize_diff(diffs),
        }

    def compare_stages(
        self,
        job_id: str,
        stage1: str = "bronze",
        stage2: str = "silver"
    ) -> Dict[str, Any]:
        """
        Compare different stages within the same job.

        Useful for seeing how bronze transforms to silver.

        Args:
            job_id: Job ID
            stage1: First stage (default: bronze)
            stage2: Second stage (default: silver)

        Returns:
            Comparison result
        """
        json1 = self.get_stage_output(job_id, stage1)
        json2 = self.get_stage_output(job_id, stage2)

        if json1 is None:
            return {"error": f"Could not load {stage1} output for job {job_id}"}
        if json2 is None:
            return {"error": f"Could not load {stage2} output for job {job_id}"}

        # For bronze vs silver, just return both (structure is too different to diff)
        return {
            "job_id": job_id,
            "stage1": stage1,
            "stage2": stage2,
            "json1": json1,
            "json2": json2,
            "note": "Bronze and silver have different structures; showing side-by-side view",
        }

    def get_available_outputs(self, job_id: str) -> Dict[str, bool]:
        """
        Check which stage outputs are available for a job.

        Args:
            job_id: Job ID

        Returns:
            Dict mapping stage -> available
        """
        return {
            "bronze": self.get_stage_output(job_id, "bronze") is not None,
            "silver": self.get_stage_output(job_id, "silver") is not None,
        }


def get_diff_service(jobs_dir: str = "./jobs") -> DiffService:
    """
    Factory function to get a DiffService instance.

    Args:
        jobs_dir: Base directory for job files

    Returns:
        DiffService instance
    """
    return DiffService(jobs_dir)
