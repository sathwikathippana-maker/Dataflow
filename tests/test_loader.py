
import json
import csv
import pytest

from pathlib import Path
from framework.loader import DataLoader


def test_write_json(tmp_path):
    loader = DataLoader()

    records = [
        {"customer_id": 1, "username": "john"},
        {"customer_id": 2, "username": "alice"}
    ]

    output_path = tmp_path / "output" / "customers.json"

    result = loader.write_json(records, output_path)

    assert Path(result).exists()

    with open(result, "r", encoding="utf-8") as file:
        saved_records = json.load(file)

    assert len(saved_records) == 2
    assert saved_records[0]["username"] == "john"


def test_write_csv(tmp_path):
    loader = DataLoader()

    records = [
        {"customer_id": 1, "username": "john"},
        {"customer_id": 2, "username": "alice"}
    ]

    output_path = tmp_path / "output" / "customers.csv"

    result = loader.write_csv(records, output_path)

    assert Path(result).exists()

    with open(result, "r", encoding="utf-8") as file:
        saved_records = list(csv.DictReader(file))

    assert len(saved_records) == 2
    assert saved_records[0]["username"] == "john"


def test_write_empty_json(tmp_path):
    loader = DataLoader()

    output_path = tmp_path / "empty.json"

    result = loader.write_json([], output_path)

    with open(result, "r", encoding="utf-8") as file:
        saved_records = json.load(file)

    assert saved_records == []


def test_write_empty_csv_raises_error(tmp_path):
    loader = DataLoader()

    output_path = tmp_path / "empty.csv"

    with pytest.raises(ValueError):
        loader.write_csv([], output_path)


def test_rerun_replaces_existing_snapshot(tmp_path):
    loader = DataLoader()

    output_path = tmp_path / "customers.json"

    first_batch = [
        {"customer_id": 1, "username": "john"},
        {"customer_id": 2, "username": "alice"}
    ]

    second_batch = [
        {"customer_id": 1, "username": "john_updated"},
        {"customer_id": 3, "username": "smith"}
    ]

    loader.write_json(first_batch, output_path)
    loader.write_json(second_batch, output_path)

    with open(output_path, "r", encoding="utf-8") as file:
        saved_records = json.load(file)

    assert len(saved_records) == 2
    assert saved_records == second_batch
    assert saved_records[0]["username"] == "john_updated"
    assert saved_records[1]["customer_id"] == 3

def test_failed_write_preserves_existing_snapshot(tmp_path, monkeypatch):
    loader = DataLoader()

    output_path = tmp_path / "customers.json"

    original_records = [
        {"customer_id": 1, "username": "john"}
    ]

    new_records = [
        {"customer_id": 2, "username": "alice"}
    ]

    # Create an existing valid output
    loader.write_json(original_records, output_path)

    # Simulate a failure during JSON writing
    def failing_dump(records, file, **kwargs):
        file.write('{"partial":')
        raise OSError("Simulated write failure")

    monkeypatch.setattr(
        "framework.loader.json.dump",
        failing_dump
    )

    # The failed write should raise an error
    with pytest.raises(OSError):
        loader.write_json(new_records, output_path)

    # Previous valid output must remain unchanged
    with open(output_path, "r", encoding="utf-8") as file:
        saved_records = json.load(file)

    assert saved_records == original_records

    # Temporary files should be cleaned up
    temp_files = list(tmp_path.glob(".*.tmp"))
    assert len(temp_files) == 0
