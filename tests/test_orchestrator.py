import json
from pathlib import Path

import pytest

from framework.loader import DataLoader
from framework.orchestrator import PipelineOrchestrator


def make_paths(tmp_path):
    return {
        "bronze_path": tmp_path / "bronze" / "customers.json",
        "silver_path": tmp_path / "silver" / "customers.json",
        "output_path": tmp_path / "gold" / "customers.json",
        "quarantine_path": tmp_path / "quarantine" / "customers.json",
        "metrics_directory": tmp_path / "metrics"
    }


def run_customers(orchestrator, paths):
    return orchestrator.run_pipeline(
        dataset_name="customers",
        source_type="api",
        url="https://example.test/customers",
        field_mapping={
            "customer_id": "id",
            "username": "username",
            "email": "email"
        },
        required_fields=["customer_id", "username", "email"],
        primary_key=["customer_id"],
        **paths
    )


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def test_pipeline_publishes_all_layers_and_quarantines_invalid_records(
    tmp_path,
    monkeypatch
):
    source_records = [
        {"id": 1, "username": "ada", "email": "ada@example.test"},
        {"id": 2, "username": "missing-email", "email": None},
        {"id": 1, "username": "duplicate", "email": "dup@example.test"}
    ]
    orchestrator = PipelineOrchestrator()
    monkeypatch.setattr(
        orchestrator.extractor,
        "extract",
        lambda *args, **kwargs: source_records
    )
    paths = make_paths(tmp_path)

    result = run_customers(orchestrator, paths)

    assert result["status"] == "SUCCESS"
    assert result["reconciliation_status"] == "PASSED"
    bronze = read_json(paths["bronze_path"])
    assert bronze[0]["source_record"] == source_records[0]
    assert bronze[0]["run_id"] == result["run_id"]
    assert len(read_json(paths["silver_path"])) == 1
    assert len(read_json(paths["output_path"])) == 1

    rejected = read_json(paths["quarantine_path"])
    assert len(rejected) == 2
    assert any("Missing or null field: email" in row["errors"] for row in rejected)
    assert any("Duplicate primary key" in row["errors"][0] for row in rejected)

    metrics = result["metrics"]
    assert metrics["source_record_count"] == 3
    assert metrics["bronze_record_count"] == 3
    assert metrics["transformed_record_count"] == 3
    assert metrics["valid_record_count"] == 1
    assert metrics["rejected_record_count"] == 2
    assert metrics["gold_output_record_count"] == 1
    assert len(list(Path(paths["metrics_directory"]).glob("*.json"))) == 1


def test_missing_source_field_is_quarantined_with_original_record(
    tmp_path,
    monkeypatch
):
    source_record = {"id": 4, "username": "no-email"}
    orchestrator = PipelineOrchestrator()
    monkeypatch.setattr(
        orchestrator.extractor,
        "extract",
        lambda *args, **kwargs: [source_record]
    )
    paths = make_paths(tmp_path)

    result = run_customers(orchestrator, paths)
    rejected = read_json(paths["quarantine_path"])[0]

    assert result["status"] == "SUCCESS"
    assert result["valid_records"] == []
    assert rejected["source_record_index"] == 1
    assert rejected["source_record"] == source_record
    assert rejected["record"]["email"] is None


def test_pipeline_flattens_cart_products_with_composite_key(
    tmp_path,
    monkeypatch
):
    source_records = [{
        "id": 5,
        "userId": 3,
        "date": "2026-11-12",
        "products": [
            {"productId": 10, "quantity": 2},
            {"productId": 11, "quantity": 1},
            {"productId": 10, "quantity": 8}
        ]
    }]
    orchestrator = PipelineOrchestrator()
    monkeypatch.setattr(
        orchestrator.extractor,
        "extract",
        lambda *args, **kwargs: source_records
    )
    paths = make_paths(tmp_path)
    paths["output_path"] = tmp_path / "gold" / "cart_items.json"
    paths["bronze_path"] = tmp_path / "bronze" / "cart_items.json"
    paths["silver_path"] = tmp_path / "silver" / "cart_items.json"
    paths["quarantine_path"] = tmp_path / "quarantine" / "cart_items.json"

    result = orchestrator.run_pipeline(
        dataset_name="cart_items",
        source_type="api",
        url="https://example.test/carts",
        nested_field="products",
        parent_mapping={
            "cart_id": "id",
            "user_id": "userId",
            "cart_date": "date"
        },
        child_mapping={
            "product_id": "productId",
            "quantity": "quantity"
        },
        required_fields=[
            "cart_id",
            "user_id",
            "cart_date",
            "product_id",
            "quantity"
        ],
        primary_key=["cart_id", "product_id"],
        **paths
    )

    assert result["status"] == "SUCCESS"
    assert result["metrics"]["transformed_record_count"] == 3
    assert result["metrics"]["valid_record_count"] == 2
    assert result["metrics"]["rejected_record_count"] == 1
    assert read_json(paths["output_path"])[1]["product_id"] == 11


def test_rerunning_pipeline_replaces_gold_snapshot(
    tmp_path,
    monkeypatch
):
    source_records = [{
        "id": 1,
        "username": "ada",
        "email": "ada@example.test"
    }]
    orchestrator = PipelineOrchestrator()
    monkeypatch.setattr(
        orchestrator.extractor,
        "extract",
        lambda *args, **kwargs: source_records
    )
    paths = make_paths(tmp_path)

    first = run_customers(orchestrator, paths)
    second = run_customers(orchestrator, paths)

    assert read_json(paths["output_path"]) == [{
        "customer_id": 1,
        "username": "ada",
        "email": "ada@example.test"
    }]
    assert first["run_id"] != second["run_id"]
    assert len(read_json(paths["bronze_path"])) == 1


def test_bronze_history_keeps_each_run_without_gold_duplicates(
    tmp_path,
    monkeypatch
):
    source_records = [{
        "id": 1,
        "username": "ada",
        "email": "ada@example.test"
    }]
    orchestrator = PipelineOrchestrator()
    monkeypatch.setattr(
        orchestrator.extractor,
        "extract",
        lambda *args, **kwargs: source_records
    )
    paths = make_paths(tmp_path)
    paths["bronze_path"] = tmp_path / "bronze" / "customers"

    first = orchestrator.run_pipeline(
        dataset_name="customers",
        source_type="json",
        source_path="customers.json",
        field_mapping={
            "customer_id": "id",
            "username": "username",
            "email": "email"
        },
        required_fields=["customer_id", "username", "email"],
        primary_key=["customer_id"],
        bronze_history=True,
        **paths
    )
    second = orchestrator.run_pipeline(
        dataset_name="customers",
        source_type="json",
        source_path="customers.json",
        field_mapping={
            "customer_id": "id",
            "username": "username",
            "email": "email"
        },
        required_fields=["customer_id", "username", "email"],
        primary_key=["customer_id"],
        bronze_history=True,
        **paths
    )

    bronze_runs = list(Path(paths["bronze_path"]).glob("*.json"))
    assert len(bronze_runs) == 2
    assert first["run_id"] != second["run_id"]
    assert len(read_json(paths["output_path"])) == 1
    assert read_json(bronze_runs[0])[0]["source_file"] == "customers.json"


def test_quarantine_includes_source_and_child_lineage(
    tmp_path,
    monkeypatch
):
    source_records = [{
        "id": 5,
        "userId": 99,
        "date": "2026-11-12T00:00:00Z",
        "products": [{"productId": 10, "quantity": 2}]
    }]
    orchestrator = PipelineOrchestrator()
    monkeypatch.setattr(
        orchestrator.extractor,
        "extract",
        lambda *args, **kwargs: source_records
    )
    paths = make_paths(tmp_path)
    paths.update({
        "bronze_path": tmp_path / "bronze" / "cart_items.json",
        "silver_path": tmp_path / "silver" / "cart_items.json",
        "output_path": tmp_path / "gold" / "cart_items.json",
        "quarantine_path": tmp_path / "quarantine" / "cart_items.json"
    })

    result = orchestrator.run_pipeline(
        dataset_name="cart_items",
        source_type="api",
        url="https://example.test/carts",
        nested_field="products",
        parent_mapping={
            "cart_id": "id",
            "user_id": "userId",
            "cart_date": "date"
        },
        child_mapping={
            "product_id": "productId",
            "quantity": "quantity"
        },
        required_fields=[
            "cart_id",
            "user_id",
            "cart_date",
            "product_id",
            "quantity"
        ],
        primary_key=["cart_id", "product_id"],
        reference_rules={
            "user_id": {
                "dataset": "customers",
                "reference_field": "customer_id",
                "values": {1, 2}
            }
        },
        **paths
    )

    rejected = read_json(paths["quarantine_path"])[0]
    assert result["status"] == "SUCCESS"
    assert rejected["source_record_index"] == 1
    assert rejected["source_child_index"] == 1
    assert rejected["source_record"] == source_records[0]
    assert any(
        "Invalid reference for user_id" in error
        for error in rejected["errors"]
    )


def test_failed_gold_write_preserves_previous_snapshot(
    tmp_path,
    monkeypatch
):
    source_records = [{
        "id": 1,
        "username": "ada",
        "email": "ada@example.test"
    }]
    orchestrator = PipelineOrchestrator()
    monkeypatch.setattr(
        orchestrator.extractor,
        "extract",
        lambda *args, **kwargs: source_records
    )
    paths = make_paths(tmp_path)
    previous = [{
        "customer_id": 9,
        "username": "prior",
        "email": "prior@example.test"
    }]
    DataLoader().write_json(previous, paths["output_path"])
    write_json = orchestrator.loader.write_json

    def fail_gold(records, file_path):
        if Path(file_path) == paths["output_path"]:
            raise OSError("simulated Gold write failure")
        return write_json(records, file_path)

    monkeypatch.setattr(orchestrator.loader, "write_json", fail_gold)

    with pytest.raises(OSError, match="simulated Gold write failure"):
        run_customers(orchestrator, paths)

    assert read_json(paths["output_path"]) == previous
    failed_metrics = list(Path(paths["metrics_directory"]).glob("*.json"))
    assert len(failed_metrics) == 1
    metrics = read_json(failed_metrics[0])
    assert metrics["status"] == "FAILED"
    assert "simulated Gold write failure" in metrics["error"]


def test_extraction_error_is_recorded_and_propagated(tmp_path, monkeypatch):
    orchestrator = PipelineOrchestrator()

    def fail_extraction(*args, **kwargs):
        raise RuntimeError("API unavailable")

    monkeypatch.setattr(
        orchestrator.extractor,
        "extract",
        fail_extraction
    )
    paths = make_paths(tmp_path)

    with pytest.raises(RuntimeError, match="API unavailable"):
        run_customers(orchestrator, paths)

    metrics = read_json(next(Path(paths["metrics_directory"]).glob("*.json")))
    assert metrics["status"] == "FAILED"
    assert metrics["error"] == "API unavailable"
