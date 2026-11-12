import json
from pathlib import Path

from framework.metrics import MetricsCollector


def test_reconciliation_failure_prevents_success(tmp_path):
    metrics = MetricsCollector()
    metrics.start_run("reconcile-test", "customers")
    metrics.record_counts(
        source_records=3,
        bronze_records=2,
        transformed_records=3,
        valid_records=2,
        rejected_records=1,
        output_records=2
    )

    metrics.finish_run("SUCCESS")
    path = metrics.save_metrics(tmp_path)

    saved = json.loads(Path(path).read_text(encoding="utf-8"))
    assert saved["status"] == "FAILED"
    assert saved["reconciliation_status"] == "FAILED"
    assert saved["reconciliation_checks"]["source_equals_bronze"] is False
