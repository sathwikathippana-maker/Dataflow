import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path


class MetricsCollector:
    """Collect and atomically persist pipeline execution metrics."""

    def __init__(self):
        self.metrics = {}

    def start_run(self, run_id, dataset_name):
        self.metrics = {
            "run_id": run_id,
            "dataset_name": dataset_name,
            "status": "RUNNING",
            "start_time": datetime.now(timezone.utc).isoformat(),
            "end_time": None,
            "duration_seconds": None,
            "source_record_count": 0,
            "bronze_record_count": 0,
            "transformed_record_count": 0,
            "valid_record_count": 0,
            "rejected_record_count": 0,
            "gold_output_record_count": 0,
            "reconciliation_status": "PENDING",
            "reconciliation_checks": {},
            "error": None,
            # Retain the original metric names for existing consumers.
            "source_records": 0,
            "input_records": 0,
            "transformed_records": 0,
            "valid_records": 0,
            "rejected_records": 0,
            "output_records": 0
        }

    def record_counts(
        self,
        source_records,
        transformed_records,
        valid_records,
        rejected_records,
        output_records,
        bronze_records=None
    ):
        self.metrics.update({
            "source_record_count": source_records,
            "bronze_record_count": bronze_records,
            "transformed_record_count": transformed_records,
            "valid_record_count": valid_records,
            "rejected_record_count": rejected_records,
            "gold_output_record_count": output_records,
            "source_records": source_records,
            "input_records": source_records,
            "transformed_records": transformed_records,
            "valid_records": valid_records,
            "rejected_records": rejected_records,
            "output_records": output_records
        })

        checks = {
            "source_equals_bronze": (
                bronze_records is not None
                and source_records == bronze_records
            ),
            "transformed_equals_valid_plus_rejected": (
                transformed_records == valid_records + rejected_records
            ),
            "valid_equals_gold_output": valid_records == output_records
        }
        self.metrics["reconciliation_checks"] = checks
        self.metrics["reconciliation_status"] = (
            "PASSED" if all(checks.values()) else "FAILED"
        )

    def finish_run(self, status=None, error=None):
        end_time = datetime.now(timezone.utc)
        self.metrics["end_time"] = end_time.isoformat()
        self.metrics["error"] = error

        start_time = datetime.fromisoformat(self.metrics["start_time"])
        self.metrics["duration_seconds"] = round(
            (end_time - start_time).total_seconds(),
            3
        )

        if status is None:
            status = (
                "SUCCESS"
                if self.metrics["reconciliation_status"] == "PASSED"
                else "FAILED"
            )
        elif (
            status == "SUCCESS"
            and self.metrics["reconciliation_status"] != "PASSED"
        ):
            status = "FAILED"

        self.metrics["status"] = status

    def save_metrics(self, directory="reports/metrics"):
        path = Path(directory)
        path.mkdir(parents=True, exist_ok=True)

        file_path = path / f"{self.metrics['run_id']}.json"
        temp_path = path / f".{file_path.name}.{uuid.uuid4().hex}.tmp"

        try:
            with temp_path.open("w", encoding="utf-8") as file:
                json.dump(self.metrics, file, indent=4)
                file.flush()
                os.fsync(file.fileno())

            os.replace(temp_path, file_path)
        finally:
            if temp_path.exists():
                temp_path.unlink()

        return str(file_path)
