
import argparse
import logging

from framework.config_loader import load_config, resolve_path
from framework.logger import setup_logger
from framework.orchestrator import PipelineOrchestrator


def _validate_mapping(
    dataset_name,
    transformation,
    required_fields,
    fail_on_schema_mismatch=True
):
    transformation_type = transformation.get("type")
    if transformation_type == "select_fields":
        output_fields = set(
            transformation.get("field_mapping", {}).keys()
        )
    elif transformation_type == "flatten_nested_records":
        output_fields = set(
            transformation.get("parent_mapping", {}).keys()
        ) | set(transformation.get("child_mapping", {}).keys())
    else:
        raise ValueError(
            f"Unsupported transformation: {transformation_type}"
        )

    missing_fields = set(required_fields) - output_fields
    if missing_fields and fail_on_schema_mismatch:
        raise ValueError(
            f"Dataset '{dataset_name}' mapping does not produce required "
            f"fields: {sorted(missing_fields)}"
        )


def _order_datasets(datasets):
    dependencies = {}
    for name, dataset in datasets.items():
        references = dataset.get("validation", {}).get("references", [])
        dependencies[name] = {
            reference["dataset"]
            for reference in references
        }

    for name, required in dependencies.items():
        if not datasets[name].get("enabled", False):
            continue
        for dependency in required:
            if dependency not in datasets or not datasets[dependency].get(
                "enabled",
                False
            ):
                raise ValueError(
                    f"Dataset '{name}' references disabled or unknown "
                    f"dataset '{dependency}'"
                )

    pending = [
        name for name, dataset in datasets.items()
        if dataset.get("enabled", False)
    ]
    ordered = []
    completed = set()

    while pending:
        ready = [
            name for name in pending
            if dependencies[name].issubset(completed)
        ]
        if not ready:
            raise ValueError("Dataset reference dependencies contain a cycle")

        for name in ready:
            ordered.append(name)
            completed.add(name)
            pending.remove(name)

    return ordered


def main(config_path=None):
    config = load_config(config_path)
    log_config = config["logging"]

    setup_logger(
        name=config["application"]["name"],
        log_directory=resolve_path(log_config["directory"]),
        level=getattr(
            logging,
            log_config["level"].upper(),
            logging.INFO
        )
    )

    logger = logging.getLogger(config["application"]["name"])
    logger.info("Starting DataFlow application")

    orchestrator = PipelineOrchestrator()
    datasets = config["datasets"]
    storage_layers = config["storage"]["layers"]
    metrics_config = config.get("metrics", {})
    validation_config = config.get("validation", {})
    pipeline_config = config.get("pipeline", {})
    any_failed = False
    published_records = {}

    for dataset_name, dataset_config in datasets.items():
        if not dataset_config.get("enabled", False):
            logger.info("Skipping disabled dataset: %s", dataset_name)

    for dataset_name in _order_datasets(datasets):
        dataset_config = datasets[dataset_name]
        logger.info("Executing dataset: %s", dataset_name)
        try:
            source = dataset_config["source"]
            transformation = dataset_config["transformation"]
            validation = dataset_config["validation"]
            required_fields = validation["required_fields"]
            _validate_mapping(
                dataset_name,
                transformation,
                required_fields,
                validation_config.get("fail_on_schema_mismatch", True)
            )

            reference_rules = {}
            for reference in validation.get("references", []):
                target_dataset = reference["dataset"]
                if target_dataset not in published_records:
                    raise ValueError(
                        f"Reference dataset '{target_dataset}' must be "
                        "enabled and successfully processed first"
                    )

                target_field = reference["reference_field"]
                reference_rules[reference["field"]] = {
                    "dataset": target_dataset,
                    "reference_field": target_field,
                    "values": {
                        record[target_field]
                        for record in published_records[target_dataset]
                        if target_field in record
                    }
                }

            output = dataset_config.get("output", {})
            gold_path = output.get("path")
            if gold_path:
                gold_path = resolve_path(gold_path)
            else:
                gold_path = resolve_path(storage_layers["gold"]) / (
                    f"{dataset_name}.json"
                )

            source_type = source.get("type", "api")
            configured_source_path = source.get("path")
            if configured_source_path:
                configured_source_path = resolve_path(configured_source_path)
            pipeline_args = {
                "dataset_name": dataset_name,
                "source_type": source_type,
                "source_path": configured_source_path,
                "url": source.get("url"),
                "records_key": source.get("records_key"),
                "source_timeout": source.get("timeout", 30),
                "source_headers": source.get("headers"),
                "field_mapping": transformation.get("field_mapping"),
                "required_fields": required_fields,
                "primary_key": validation["primary_key"],
                "output_path": gold_path,
                "bronze_path": (
                    resolve_path(storage_layers["bronze"])
                    / dataset_name
                ),
                "silver_path": (
                    resolve_path(storage_layers["silver"])
                    / f"{dataset_name}.json"
                ),
                "quarantine_path": (
                    resolve_path(storage_layers["quarantine"])
                    / f"{dataset_name}.json"
                ),
                "metrics_directory": resolve_path(
                    metrics_config.get("directory", "reports/metrics")
                ),
                "metrics_enabled": metrics_config.get("enabled", True),
                "quarantine_invalid_records": validation_config.get(
                    "quarantine_invalid_records",
                    True
                ),
                "validation_options": validation_config,
                "field_types": validation.get("field_types"),
                "minimum_values": validation.get("minimum_values"),
                "reference_rules": reference_rules,
                "save_run_metadata": pipeline_config.get(
                    "save_run_metadata",
                    True
                ),
                "bronze_history": pipeline_config.get(
                    "bronze_history",
                    True
                )
            }

            if transformation["type"] == "flatten_nested_records":
                pipeline_args.update({
                    "nested_field": transformation["nested_field"],
                    "parent_mapping": transformation["parent_mapping"],
                    "child_mapping": transformation["child_mapping"]
                })

            result = orchestrator.run_pipeline(**pipeline_args)
            logger.info(
                "Dataset result: %s | Status: %s",
                dataset_name,
                result["status"]
            )
            if result["status"] != "SUCCESS":
                any_failed = True
            else:
                published_records[dataset_name] = result["valid_records"]
        except Exception:
            any_failed = True
            logger.exception("Dataset execution failed: %s", dataset_name)

    if any_failed:
        logger.error("DataFlow completed with one or more failed datasets")
        return 1

    logger.info("DataFlow application completed successfully")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the DataFlow pipeline")
    parser.add_argument(
        "--config",
        help="Path to a pipeline YAML file (defaults to config/pipeline_config.yaml)"
    )
    arguments = parser.parse_args()
    raise SystemExit(main(arguments.config))
