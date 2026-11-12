import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path

from framework.extractor import DataExtractor
from framework.loader import DataLoader
from framework.metrics import MetricsCollector
from framework.transformer import DataTransformer
from framework.validator import DataValidator


class PipelineOrchestrator:
    def __init__(self):
        self.logger = logging.getLogger("DataFlow")
        self.extractor = DataExtractor()
        self.transformer = DataTransformer()
        self.validator = DataValidator()
        self.loader = DataLoader()
        self.metrics = MetricsCollector()

    def run_pipeline(
        self,
        dataset_name,
        url=None,
        records_key=None,
        field_mapping=None,
        required_fields=None,
        primary_key=None,
        output_path=None,
        nested_field=None,
        parent_mapping=None,
        child_mapping=None,
        source_type="api",
        source_path=None,
        source_timeout=30,
        source_headers=None,
        bronze_path=None,
        silver_path=None,
        quarantine_path=None,
        metrics_directory="reports/metrics",
        metrics_enabled=True,
        quarantine_invalid_records=True,
        validation_options=None,
        save_run_metadata=True,
        field_types=None,
        minimum_values=None,
        reference_rules=None,
        bronze_history=False
    ):
        run_id = str(uuid.uuid4())
        self.metrics.start_run(run_id, dataset_name)
        validation_options = validation_options or {}

        try:
            if not output_path:
                raise ValueError("A Gold output path is required")
            if not bronze_path or not silver_path or not quarantine_path:
                raise ValueError(
                    "Bronze, Silver, and quarantine paths are required"
                )
            if required_fields is None or primary_key is None:
                raise ValueError(
                    "Required fields and primary keys must be configured"
                )

            self.logger.info(
                "Starting pipeline: %s | Run: %s",
                dataset_name,
                run_id
            )

            source = source_path if source_path is not None else url
            records = self.extractor.extract(
                source_type,
                source,
                records_key=records_key,
                timeout=source_timeout,
                headers=source_headers
            )
            self.metrics.record_counts(
                source_records=len(records),
                bronze_records=0,
                transformed_records=0,
                valid_records=0,
                rejected_records=0,
                output_records=0
            )

            if save_run_metadata:
                ingestion_timestamp = datetime.now(timezone.utc).isoformat()
                bronze_destination = Path(bronze_path)
                if bronze_history:
                    bronze_destination = (
                        bronze_destination / f"{run_id}.json"
                    )
                bronze_records = [
                    {
                        "run_id": run_id,
                        "ingestion_timestamp": ingestion_timestamp,
                        "source_type": source_type,
                        "source_uri": str(source),
                        "source_file": (
                            str(source)
                            if source_type in ("csv", "json")
                            else None
                        ),
                        "source_record": record
                    }
                    for record in records
                ]
            else:
                bronze_records = records
                bronze_destination = Path(bronze_path)
                if bronze_history:
                    bronze_destination = (
                        bronze_destination / f"{run_id}.json"
                    )
            self.loader.write_json(bronze_records, bronze_destination)
            self.metrics.record_counts(
                source_records=len(records),
                bronze_records=len(bronze_records),
                transformed_records=0,
                valid_records=0,
                rejected_records=0,
                output_records=0
            )

            if nested_field is not None:
                transformed = self.transformer.flatten_nested_records(
                    records,
                    nested_field,
                    parent_mapping or {},
                    child_mapping or {},
                    include_lineage=True
                )
            else:
                transformed = self.transformer.select_fields(
                    records,
                    field_mapping or {},
                    include_lineage=True
                )

            result = self.validator.validate(
                transformed,
                required_fields,
                primary_key,
                check_nulls=validation_options.get("check_nulls", True),
                check_duplicates=validation_options.get(
                    "check_duplicates",
                    True
                ),
                field_types=field_types,
                minimum_values=minimum_values,
                references=reference_rules
            )
            validated_records = result["valid_records"]
            rejected_records = result["rejected_records"]
            valid_records = [
                self._strip_internal_fields(record)
                for record in validated_records
            ]
            quarantine_records = [
                self._quarantine_record(
                    rejection,
                    records,
                    run_id
                )
                for rejection in rejected_records
            ]

            self.metrics.record_counts(
                source_records=len(records),
                bronze_records=len(bronze_records),
                transformed_records=len(transformed),
                valid_records=len(valid_records),
                rejected_records=len(quarantine_records),
                output_records=0
            )

            if quarantine_records and not quarantine_invalid_records:
                raise ValueError(
                    f"{len(quarantine_records)} records were rejected, "
                    "but quarantine is disabled"
                )

            self.loader.write_json(valid_records, silver_path)
            self.loader.write_json(quarantine_records, quarantine_path)
            self.loader.write_json(valid_records, output_path)

            self.metrics.record_counts(
                source_records=len(records),
                bronze_records=len(bronze_records),
                transformed_records=len(transformed),
                valid_records=len(valid_records),
                rejected_records=len(quarantine_records),
                output_records=len(valid_records)
            )

            if self.metrics.metrics["reconciliation_status"] == "PASSED":
                self.metrics.finish_run("SUCCESS")
            else:
                self.metrics.finish_run(
                    status="FAILED",
                    error="Reconciliation checks failed"
                )
                self.logger.error(
                    "Pipeline reconciliation failed: %s | Run: %s",
                    dataset_name,
                    run_id
                )

            self.logger.info(
                "Pipeline completed: %s | Status: %s | Valid: %s | "
                "Rejected: %s",
                dataset_name,
                self.metrics.metrics["status"],
                len(valid_records),
                len(quarantine_records)
            )

            result["valid_records"] = valid_records
            result["rejected_records"] = quarantine_records
            return {
                "run_id": run_id,
                "dataset": dataset_name,
                "status": self.metrics.metrics["status"],
                "validation": result,
                "valid_records": valid_records,
                "bronze_path": str(bronze_destination),
                "reconciliation_status": (
                    self.metrics.metrics["reconciliation_status"]
                ),
                "metrics": dict(self.metrics.metrics)
            }

        except Exception as error:
            self.metrics.finish_run(
                status="FAILED",
                error=str(error)
            )
            self.logger.exception(
                "Pipeline failed: %s | Run: %s",
                dataset_name,
                run_id
            )
            raise

        finally:
            if metrics_enabled:
                self.metrics.save_metrics(metrics_directory)

    @staticmethod
    def _strip_internal_fields(record):
        return {
            field: value
            for field, value in record.items()
            if not field.startswith("_dataflow_")
        }

    def _quarantine_record(self, rejection, source_records, run_id):
        record = rejection["record"]
        source_record_index = record.get("_dataflow_source_record_index")
        source_child_index = record.get("_dataflow_source_child_index")

        source_record = None
        if (
            isinstance(source_record_index, int)
            and 1 <= source_record_index <= len(source_records)
        ):
            source_record = source_records[source_record_index - 1]

        result = {
            "run_id": run_id,
            "row_number": rejection["row_number"],
            "source_record_index": source_record_index,
            "source_record": source_record,
            "record": self._strip_internal_fields(record),
            "errors": rejection["errors"]
        }
        if source_child_index is not None:
            result["source_child_index"] = source_child_index
        return result
