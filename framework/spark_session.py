
import logging
from pathlib import Path


logger = logging.getLogger(__name__)


def _load_spark_dependencies():
    try:
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession
    except ImportError as error:
        raise RuntimeError(
            "The optional Spark backend requires PySpark and Delta Lake. "
            "Install them with `python -m pip install -r requirements-spark.txt`."
        ) from error

    return configure_spark_with_delta_pip, SparkSession


def create_spark_session(
    app_name="DataFlow",
    master="local[*]",
    warehouse_path=None
):
    """Create an optional Spark session configured with Delta Lake."""
    configure_spark_with_delta_pip, SparkSession = (
        _load_spark_dependencies()
    )

    project_root = Path(__file__).resolve().parent.parent
    warehouse_path = Path(warehouse_path) if warehouse_path else (
        project_root / "data" / "spark-warehouse"
    )
    if not warehouse_path.is_absolute():
        warehouse_path = project_root / warehouse_path
    warehouse_path = warehouse_path.resolve()
    warehouse_path.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config(
            "spark.sql.warehouse.dir",
            warehouse_path.as_uri()
        )
        .config(
            "spark.driver.host",
            "127.0.0.1"
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(
        "Spark session started. Version: %s",
        spark.version
    )

    return spark


if __name__ == "__main__":
    spark = create_spark_session()
    print("Spark session created successfully")
    print("Spark version:", spark.version)
    spark.stop()
