import json
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from dashboard import _failed_reports, _previous_run
from framework.dashboard_data import load_layer_records, load_metrics


def test_load_metrics_orders_newest_first(tmp_path):
    reports = [
        {"run_id": "older", "start_time": "2026-01-01T00:00:00+00:00"},
        {"run_id": "newer", "start_time": "2026-02-01T00:00:00+00:00"}
    ]
    for report in reports:
        (tmp_path / f"{report['run_id']}.json").write_text(
            json.dumps(report),
            encoding="utf-8"
        )

    assert [row["run_id"] for row in load_metrics(tmp_path)] == [
        "newer",
        "older"
    ]


def test_previous_run_returns_the_next_older_run():
    newest = {"run_id": "newest"}
    older = {"run_id": "older"}
    reports = [newest, older]

    assert _previous_run(reports, newest) is older
    assert _previous_run(reports, older) is None


def test_failed_reports_finds_failed_runs_case_insensitively():
    reports = [
        {"run_id": "success", "status": "SUCCESS"},
        {"run_id": "failed", "status": "FAILED"},
        {"run_id": "legacy-failed", "status": "failed"}
    ]

    assert [report["run_id"] for report in _failed_reports(reports)] == [
        "failed",
        "legacy-failed"
    ]


def test_load_layer_records_reads_selected_bronze_run_and_gold(tmp_path):
    bronze_dir = tmp_path / "bronze" / "customers"
    bronze_dir.mkdir(parents=True)
    bronze_file = bronze_dir / "run-1.json"
    bronze_file.write_text(json.dumps([{"source_record": {"id": 1}}]), encoding="utf-8")

    gold_file = tmp_path / "gold" / "customers.json"
    gold_file.parent.mkdir(parents=True)
    gold_file.write_text(json.dumps([{"customer_id": 1}]), encoding="utf-8")

    config = {
        "storage": {
            "layers": {
                "bronze": str(tmp_path / "bronze"),
                "silver": str(tmp_path / "silver"),
                "gold": str(tmp_path / "gold"),
                "quarantine": str(tmp_path / "quarantine")
            }
        },
        "datasets": {
            "customers": {"output": {"path": str(gold_file)}}
        }
    }

    assert load_layer_records(
        config,
        "customers",
        "bronze",
        run_id="run-1"
    ) == [{"source_record": {"id": 1}}]
    assert load_layer_records(config, "customers", "gold") == [
        {"customer_id": 1}
    ]


def test_load_layer_records_rejects_unknown_layer(tmp_path):
    config = {
        "storage": {"layers": {}},
        "datasets": {"customers": {}}
    }

    with pytest.raises(ValueError, match="Unsupported data layer"):
        load_layer_records(config, "customers", "platinum")


def test_dashboard_renders_without_exceptions():
    app_path = Path(__file__).resolve().parents[1] / "dashboard.py"
    app = AppTest.from_file(app_path, default_timeout=15).run()

    assert not app.exception
