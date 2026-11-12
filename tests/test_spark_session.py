import builtins

import pytest

from framework import spark_session


def test_spark_dependencies_are_optional_with_an_actionable_error(monkeypatch):
    original_import = builtins.__import__

    def import_without_spark(name, *args, **kwargs):
        if name == "delta" or name.startswith("pyspark"):
            raise ImportError("blocked optional dependency")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_without_spark)

    with pytest.raises(RuntimeError, match="requirements-spark.txt"):
        spark_session.create_spark_session()


def test_create_spark_session_applies_delta_and_runtime_settings(
    tmp_path,
    monkeypatch
):
    calls = []

    class FakeBuilder:
        def appName(self, value):
            calls.append(("appName", value))
            return self

        def master(self, value):
            calls.append(("master", value))
            return self

        def config(self, key, value):
            calls.append((key, value))
            return self

        def getOrCreate(self):
            return fake_spark

    class FakeSparkContext:
        def setLogLevel(self, value):
            calls.append(("log_level", value))

    class FakeSpark:
        version = "test-version"
        sparkContext = FakeSparkContext()

    fake_spark = FakeSpark()

    class FakeSparkSession:
        builder = FakeBuilder()

    def configure_delta(builder):
        calls.append(("delta_configured", True))
        return builder

    monkeypatch.setattr(
        spark_session,
        "_load_spark_dependencies",
        lambda: (configure_delta, FakeSparkSession)
    )

    warehouse = tmp_path / "spark-warehouse"
    result = spark_session.create_spark_session(
        app_name="DataFlowTest",
        master="local[2]",
        warehouse_path=warehouse
    )

    assert result is fake_spark
    assert warehouse.is_dir()
    assert ("appName", "DataFlowTest") in calls
    assert ("master", "local[2]") in calls
    assert (
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    ) in calls
    assert ("delta_configured", True) in calls
    assert ("log_level", "WARN") in calls
