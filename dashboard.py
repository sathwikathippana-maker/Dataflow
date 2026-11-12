import json

import streamlit as st
import yaml

from framework.config_loader import load_config, resolve_path
from framework.dashboard_data import load_layer_records, load_metrics


def _previous_run(dataset_reports, selected_report):
    selected_run_id = selected_report.get("run_id")
    for index, report in enumerate(dataset_reports):
        if report is selected_report or (
            selected_run_id
            and report.get("run_id") == selected_run_id
        ):
            return (
                dataset_reports[index + 1]
                if index + 1 < len(dataset_reports)
                else None
            )
    return None


def _failed_reports(reports):
    return [
        report for report in reports
        if str(report.get("status", "")).upper() == "FAILED"
    ]


def _report_count(report, current_key, legacy_key=None):
    value = report.get(current_key)
    if value is None and legacy_key:
        value = report.get(legacy_key)
    return value if value is not None else 0


def _show_fields(record):
    fields = list(record.items())
    if not fields:
        st.caption("No fields to display.")
        return

    columns = st.columns(min(len(fields), 2))
    for index, (name, value) in enumerate(fields):
        with columns[index % len(columns)]:
            st.caption(name.replace("_", " ").title())
            if isinstance(value, (dict, list)):
                st.code(json.dumps(value, indent=2, ensure_ascii=False))
            else:
                st.write(value if value is not None else "Not set")


def _show_record(record, title):
    with st.container(border=True):
        st.markdown(f"**{title}**")
        _show_fields(record)


def _show_reconciliation(checks):
    names = {
        "source_equals_bronze": "Source count = Bronze count",
        "transformed_equals_valid_plus_rejected": (
            "Transformed = valid + rejected"
        ),
        "valid_equals_gold_output": "Valid count = Gold count"
    }
    columns = st.columns(3)

    for index, (key, label) in enumerate(names.items()):
        passed = checks.get(key)
        with columns[index]:
            if passed is True:
                st.success(f"PASS\n\n{label}")
            elif passed is False:
                st.error(f"FAIL\n\n{label}")
            else:
                st.warning(f"NOT RECORDED\n\n{label}")


def _show_quarantine(records):
    if not records:
        st.success("No quarantined rows in the current snapshot.")
        return

    st.warning(f"{len(records)} rejected row(s) in the current snapshot.")
    for index, rejection in enumerate(records[:20], start=1):
        errors = rejection.get("errors", [])
        reason = "; ".join(errors) if errors else "No reason recorded"
        with st.expander(
            f"Reject {index} · {reason}",
            expanded=index == 1
        ):
            st.caption(
                f"Run {rejection.get('run_id', 'unknown')} · "
                f"source row {rejection.get('source_record_index', 'unknown')} "
                f"· child {rejection.get('source_child_index', 'n/a')}"
            )
            for error in errors:
                st.error(error)

            source_column, record_column = st.columns(2)
            with source_column:
                st.markdown("**Original source record**")
                source_record = rejection.get("source_record")
                if isinstance(source_record, dict):
                    _show_fields(source_record)
                else:
                    st.caption("Original source record is unavailable.")
            with record_column:
                st.markdown("**Transformed record**")
                transformed_record = rejection.get("record", {})
                if isinstance(transformed_record, dict):
                    _show_fields(transformed_record)

    if len(records) > 20:
        st.caption("Showing the first 20 rejects.")


def main():
    st.set_page_config(page_title="DataFlow", page_icon="DF", layout="wide")
    st.markdown(
        """
        <style>
        .block-container {
            box-sizing: border-box;
            max-width: 100%;
            padding: 1.2rem clamp(0.75rem, 3vw, 2rem);
        }
        .df-hero {
            padding: 1.25rem 1.5rem;
            border-radius: 0.8rem;
            border: 1px solid rgba(128, 128, 128, 0.25);
            background: linear-gradient(110deg, rgba(42, 105, 156, 0.16), rgba(42, 105, 156, 0.03));
            margin-bottom: 1.2rem;
        }
        .df-eyebrow {
            text-transform: uppercase;
            letter-spacing: 0.12em;
            font-size: 0.72rem;
            opacity: 0.72;
        }
        .df-hero h1 { margin: 0.3rem 0; }
        .df-hero p { margin: 0; opacity: 0.82; }
        @media (max-width: 800px) {
            .block-container { padding: 1rem 0.75rem; }
            .df-hero { padding: 1rem; }
            .df-eyebrow { letter-spacing: 0.08em; }
        }
        </style>
        <div class="df-hero">
            <div class="df-eyebrow">Local pipeline observability</div>
            <h1>DataFlow</h1>
            <p>Run health, reconciliation, and data quality at a glance.</p>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.sidebar.header("View")
    config_path = st.sidebar.text_input(
        "Pipeline config",
        value="config/large_sample_pipeline_config.yaml"
    )

    try:
        config = load_config(config_path)
        metrics_directory = resolve_path(
            config.get("metrics", {}).get("directory", "reports/metrics")
        )
        reports = load_metrics(metrics_directory)
    except (OSError, ValueError, yaml.YAMLError) as error:
        st.error(f"Could not load configuration or metrics: {error}")
        st.stop()

    if not reports:
        st.info("No metrics reports found. Run the pipeline to create one.")
        return

    dataset_names = sorted({
        report.get("dataset_name", "unknown")
        for report in reports
    })
    dataset_name = st.sidebar.selectbox("Dataset", dataset_names)
    dataset_reports = [
        report for report in reports
        if report.get("dataset_name", "unknown") == dataset_name
    ]
    selected_report = st.sidebar.selectbox(
        "Run",
        dataset_reports,
        format_func=lambda report: (
            f"{report.get('status', 'UNKNOWN')} · "
            f"{report.get('start_time', 'unknown time')} · "
            f"{str(report.get('run_id', 'unknown run'))[:8]}"
        )
    )

    status = selected_report.get("status", "UNKNOWN")
    if status == "SUCCESS":
        st.success("Pipeline completed successfully")
    elif status == "FAILED":
        st.error("Pipeline failed")
    else:
        st.warning(f"Pipeline status: {status}")

    info_columns = st.columns(3)
    info_columns[0].metric(
        "Duration",
        f"{selected_report.get('duration_seconds', 0)} sec"
    )
    info_columns[1].metric(
        "Reconciliation",
        selected_report.get("reconciliation_status", "UNKNOWN")
    )
    info_columns[2].metric(
        "Run ID",
        str(selected_report.get("run_id", "unknown"))[:8]
    )

    st.subheader("Record flow")
    count_metrics = [
        (
            "Source",
            "source_record_count",
            "source_records"
        ),
        ("Bronze", "bronze_record_count", None),
        (
            "Transformed",
            "transformed_record_count",
            "transformed_records"
        ),
        ("Valid", "valid_record_count", "valid_records"),
        ("Rejected", "rejected_record_count", "rejected_records"),
        ("Gold", "gold_output_record_count", "output_records")
    ]
    for row_start in (0, 3):
        count_columns = st.columns(3)
        for column, (label, current_key, legacy_key) in zip(
            count_columns,
            count_metrics[row_start:row_start + 3]
        ):
            value = selected_report.get(current_key)
            if value is None and legacy_key:
                value = selected_report.get(legacy_key, 0)
            column.metric(label, value if value is not None else 0)

    previous_report = _previous_run(dataset_reports, selected_report)
    st.subheader("Previous run")
    if previous_report is None:
        st.info("No earlier run is recorded for this dataset.")
    else:
        st.caption(
            f"{previous_report.get('start_time', 'Unknown time')} · "
            f"Run ID {str(previous_report.get('run_id', 'unknown'))[:8]}"
        )
        previous_columns = st.columns(4)
        previous_columns[0].metric(
            "Previous status",
            previous_report.get("status", "UNKNOWN")
        )
        previous_columns[1].metric(
            "Previous duration",
            f"{previous_report.get('duration_seconds', 'unknown')} sec"
        )
        previous_columns[2].metric(
            "Previous Gold rows",
            f"{_report_count(previous_report, 'gold_output_record_count', 'output_records'):,}",
            delta=(
                f"{_report_count(selected_report, 'gold_output_record_count', 'output_records') - _report_count(previous_report, 'gold_output_record_count', 'output_records'):+,} vs selected"
            ),
            delta_color="off"
        )
        previous_columns[3].metric(
            "Previous rejects",
            f"{_report_count(previous_report, 'rejected_record_count', 'rejected_records'):,}",
            delta=(
                f"{_report_count(selected_report, 'rejected_record_count', 'rejected_records') - _report_count(previous_report, 'rejected_record_count', 'rejected_records'):+,} vs selected"
            ),
            delta_color="off"
        )

    st.subheader("Reconciliation")
    checks = selected_report.get("reconciliation_checks") or {}
    _show_reconciliation(checks)
    if selected_report.get("error"):
        st.error(selected_report["error"])

    failed_reports = _failed_reports(reports)
    st.subheader(f"Pipeline failures ({len(failed_reports)})")
    if not failed_reports:
        st.success("No failed pipeline runs are recorded in this metrics history.")
    else:
        for report in failed_reports[:10]:
            failure_label = (
                f"{report.get('dataset_name', 'unknown')} · "
                f"{report.get('start_time', 'unknown time')} · "
                f"{str(report.get('run_id', 'unknown'))[:8]}"
            )
            with st.expander(failure_label):
                st.error(
                    report.get("error")
                    or (
                        "Reconciliation failed."
                        if report.get("reconciliation_status") == "FAILED"
                        else "No error details were recorded for this run."
                    )
                )
                st.caption(
                    f"Run ID: {report.get('run_id', 'unknown')} · "
                    f"Duration: {report.get('duration_seconds', 'unknown')} sec"
                )
                st.write(
                    f"Source: {_report_count(report, 'source_record_count', 'source_records'):,} · "
                    f"Valid: {_report_count(report, 'valid_record_count', 'valid_records'):,} · "
                    f"Rejected: {_report_count(report, 'rejected_record_count', 'rejected_records'):,} · "
                    f"Gold: {_report_count(report, 'gold_output_record_count', 'output_records'):,}"
                )
        if len(failed_reports) > 10:
            st.caption("Showing the 10 most recent failures.")

    selected_run_id = selected_report.get("run_id")
    gold_records = load_layer_records(config, dataset_name, "gold")
    quarantine_records = load_layer_records(
        config,
        dataset_name,
        "quarantine"
    )
    bronze_records = load_layer_records(
        config,
        dataset_name,
        "bronze",
        run_id=selected_run_id
    )

    gold_tab, quarantine_tab, bronze_tab = st.tabs([
        "Gold preview",
        f"Quarantine ({len(quarantine_records)})",
        "Bronze run"
    ])

    with gold_tab:
        st.caption("Gold shows the current published snapshot.")
        if not gold_records:
            st.info("No Gold output is available.")
        else:
            preview_count = st.slider(
                "Rows to preview",
                min_value=1,
                max_value=min(20, len(gold_records)),
                value=min(5, len(gold_records))
            )
            for index, record in enumerate(gold_records[:preview_count], start=1):
                _show_record(record, f"Gold row {index}")
            st.caption(
                f"Showing {min(preview_count, len(gold_records))} of "
                f"{len(gold_records)} rows."
            )

    with quarantine_tab:
        st.caption("Each rejection includes its reason and source lineage.")
        _show_quarantine(quarantine_records)

    with bronze_tab:
        st.caption("Bronze is the immutable source snapshot for the selected run.")
        if not bronze_records:
            st.info("No Bronze records found for this run.")
        else:
            for index, bronze_record in enumerate(bronze_records[:10], start=1):
                source_record = bronze_record.get(
                    "source_record",
                    bronze_record
                )
                with st.expander(f"Source record {index}"):
                    st.caption(
                        f"Ingested {bronze_record.get('ingestion_timestamp', 'unknown')} · "
                        f"source {bronze_record.get('source_file') or bronze_record.get('source_uri', 'unknown')}"
                    )
                    if isinstance(source_record, dict):
                        _show_fields(source_record)
                    else:
                        st.write(source_record)
            if len(bronze_records) > 10:
                st.caption("Showing the first 10 source records.")

    st.subheader("Recent run history")
    for report in reports[:10]:
        run_status = report.get("status", "UNKNOWN")
        run_label = (
            f"{report.get('dataset_name', 'unknown')} · "
            f"{run_status} · "
            f"{report.get('start_time', 'unknown time')}"
        )
        with st.expander(run_label):
            history_columns = st.columns(4)
            history_columns[0].write(
                f"Run ID: {report.get('run_id', 'unknown')}"
            )
            history_columns[1].write(
                f"Duration: {report.get('duration_seconds', 'unknown')} sec"
            )
            history_columns[2].write(
                f"Reconciliation: "
                f"{report.get('reconciliation_status', 'unknown')}"
            )
            history_columns[3].write(
                f"Rejected: "
                f"{report.get('rejected_record_count', report.get('rejected_records', 0))}"
            )


if __name__ == "__main__":
    main()
