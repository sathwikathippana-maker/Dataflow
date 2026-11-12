import json
from pathlib import Path

from framework.config_loader import resolve_path


def load_metrics(directory):
    metrics = []
    for path in Path(directory).glob("*.json"):
        with path.open("r", encoding="utf-8") as file:
            report = json.load(file)
        if not isinstance(report, dict):
            raise ValueError(f"Metrics report must be an object: {path}")
        metrics.append(report)

    return sorted(
        metrics,
        key=lambda report: report.get("start_time") or "",
        reverse=True
    )


def load_layer_records(config, dataset_name, layer, run_id=None):
    dataset_config = config["datasets"][dataset_name]
    layer_paths = config["storage"]["layers"]

    if layer == "gold":
        configured_path = dataset_config.get("output", {}).get("path")
        path = (
            resolve_path(configured_path)
            if configured_path
            else resolve_path(layer_paths["gold"]) / f"{dataset_name}.json"
        )
    elif layer == "bronze":
        bronze_root = resolve_path(layer_paths["bronze"])
        history_directory = bronze_root / dataset_name
        if run_id:
            path = history_directory / f"{run_id}.json"
        elif history_directory.is_dir():
            history = sorted(
                history_directory.glob("*.json"),
                key=lambda item: item.stat().st_mtime,
                reverse=True
            )
            path = history[0] if history else bronze_root / f"{dataset_name}.json"
        else:
            path = bronze_root / f"{dataset_name}.json"
    elif layer in ("silver", "quarantine"):
        path = resolve_path(layer_paths[layer]) / f"{dataset_name}.json"
    else:
        raise ValueError(f"Unsupported data layer: {layer}")

    if not path.is_file():
        return []

    with path.open("r", encoding="utf-8") as file:
        records = json.load(file)

    if isinstance(records, dict):
        return [records]
    if isinstance(records, list):
        return records
    raise ValueError(f"Layer data must be a JSON object or array: {path}")
