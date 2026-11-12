import json

import yaml

import main as dataflow_main


def write_config(path, source_path, output_root, logging_root, metrics_root):
    config = {
        "application": {"name": "DataFlowTest", "environment": "test"},
        "storage": {
            "layers": {
                "bronze": str(output_root / "bronze"),
                "silver": str(output_root / "silver"),
                "gold": str(output_root / "gold"),
                "quarantine": str(output_root / "quarantine")
            }
        },
        "logging": {"level": "ERROR", "directory": str(logging_root)},
        "metrics": {"enabled": True, "directory": str(metrics_root)},
        "validation": {
            "fail_on_schema_mismatch": True,
            "quarantine_invalid_records": True,
            "check_nulls": True,
            "check_duplicates": True
        },
        "pipeline": {"save_run_metadata": True},
        "datasets": {
            "customers": {
                "enabled": True,
                "source": {
                    "type": "json",
                    "path": str(source_path)
                },
                "transformation": {
                    "type": "select_fields",
                    "field_mapping": {
                        "customer_id": "id",
                        "username": "username",
                        "email": "email"
                    }
                },
                "validation": {
                    "required_fields": [
                        "customer_id",
                        "username",
                        "email"
                    ],
                    "primary_key": ["customer_id"]
                },
                "output": {
                    "path": str(output_root / "gold" / "customers.json")
                }
            }
        }
    }
    path.write_text(yaml.safe_dump(config), encoding="utf-8")


def test_main_runs_using_configured_paths(tmp_path):
    source_path = tmp_path / "customers.json"
    source_path.write_text(json.dumps([
        {"id": 1, "username": "ada", "email": "ada@example.test"}
    ]), encoding="utf-8")
    config_path = tmp_path / "pipeline.yaml"
    write_config(
        config_path,
        source_path,
        tmp_path / "data",
        tmp_path / "logs",
        tmp_path / "metrics"
    )

    assert dataflow_main.main(config_path) == 0
    assert json.loads(
        (tmp_path / "data" / "gold" / "customers.json").read_text(
            encoding="utf-8"
        )
    ) == [{
        "customer_id": 1,
        "username": "ada",
        "email": "ada@example.test"
    }]


def test_main_continues_after_one_dataset_fails(tmp_path):
    source_path = tmp_path / "customers.json"
    source_path.write_text(json.dumps([
        {"id": 1, "username": "ada", "email": "ada@example.test"}
    ]), encoding="utf-8")
    config_path = tmp_path / "pipeline.yaml"
    write_config(
        config_path,
        source_path,
        tmp_path / "data",
        tmp_path / "logs",
        tmp_path / "metrics"
    )
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    broken = dict(config["datasets"]["customers"])
    broken["source"] = {
        "type": "json",
        "path": str(tmp_path / "missing.json")
    }
    config["datasets"] = {
        "broken_customers": broken,
        "customers": config["datasets"]["customers"]
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    assert dataflow_main.main(config_path) == 1
    assert (tmp_path / "data" / "gold" / "customers.json").exists()


def test_main_checks_cart_customer_references_and_keeps_lineage(tmp_path):
    customer_source = tmp_path / "customers.json"
    customer_source.write_text(json.dumps([
        {"id": 1, "username": "ada", "email": "ada@example.test"}
    ]), encoding="utf-8")
    cart_source = tmp_path / "carts.json"
    carts = [
        {
            "id": 10,
            "userId": 1,
            "date": "2026-11-12T00:00:00Z",
            "products": [{"productId": 2, "quantity": 1}]
        },
        {
            "id": 11,
            "userId": 99,
            "date": "2026-11-12T00:00:00Z",
            "products": [{"productId": 3, "quantity": 2}]
        }
    ]
    cart_source.write_text(json.dumps(carts), encoding="utf-8")
    config_path = tmp_path / "pipeline.yaml"
    write_config(
        config_path,
        customer_source,
        tmp_path / "data",
        tmp_path / "logs",
        tmp_path / "metrics"
    )
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["datasets"]["cart_items"] = {
        "enabled": True,
        "source": {"type": "json", "path": str(cart_source)},
        "transformation": {
            "type": "flatten_nested_records",
            "nested_field": "products",
            "parent_mapping": {
                "cart_id": "id",
                "user_id": "userId",
                "cart_date": "date"
            },
            "child_mapping": {
                "product_id": "productId",
                "quantity": "quantity"
            }
        },
        "validation": {
            "required_fields": [
                "cart_id",
                "user_id",
                "cart_date",
                "product_id",
                "quantity"
            ],
            "primary_key": ["cart_id", "product_id"],
            "field_types": {
                "cart_id": "integer",
                "user_id": "integer",
                "cart_date": "datetime",
                "product_id": "integer",
                "quantity": "integer"
            },
            "minimum_values": {"quantity": 1},
            "references": [{
                "field": "user_id",
                "dataset": "customers",
                "reference_field": "customer_id"
            }]
        },
        "output": {
            "path": str(tmp_path / "data" / "gold" / "cart_items.json")
        }
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    assert dataflow_main.main(config_path) == 0
    gold = json.loads(
        (tmp_path / "data" / "gold" / "cart_items.json").read_text(
            encoding="utf-8"
        )
    )
    quarantine = json.loads(
        (tmp_path / "data" / "quarantine" / "cart_items.json").read_text(
            encoding="utf-8"
        )
    )

    assert len(gold) == 1
    assert len(quarantine) == 1
    assert quarantine[0]["source_record_index"] == 2
    assert quarantine[0]["source_child_index"] == 1
    assert quarantine[0]["source_record"] == carts[1]
