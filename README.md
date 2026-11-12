# DataFlow

DataFlow is a local, Python-based data engineering framework. The default
pipeline does not start Spark and does not require Hadoop or `winutils.exe`.
Spark/Delta support remains isolated in `framework/spark_session.py`.

## Architecture

For each enabled dataset, `main.py` reads `config/pipeline_config.yaml` and
executes:

1. **Extract** from a configured API, CSV, or JSON source.
2. **Bronze**: preserve each source extraction under a run-specific file with
   run ID, source URI/file, and ingestion timestamp metadata.
3. **Silver**: apply the configured field-selection or nested-record
   flattening strategy, validate required fields and keys, and publish valid
   transformed records.
4. **Quarantine**: publish rejected Silver records with row numbers and
   validation reasons.
5. **Gold**: atomically publish the clean business-ready records.
6. **Metrics**: write a JSON run report and reconcile source/Bronze,
   transformed/valid-plus-rejected, and valid/Gold counts.

Gold files are snapshots, not append-only outputs. Reruns replace each
dataset's current files, preventing duplicate records from accumulating.
Each file is replaced atomically; publishing multiple layer files is not a
single cross-file transaction.

## Output layout

The configured defaults write:

```text
data/
  bronze/customers/<run_id>.json
  bronze/cart_items/<run_id>.json
  silver/customers.json
  silver/cart_items.json
  gold/customers.json
  gold/cart_items.json
  quarantine/customers.json
  quarantine/cart_items.json
reports/
  logs/dataflow.log
  metrics/<run_id>.json
```

Bronze rows wrap each untouched source object in `source_record` and include
`run_id`, `source_uri`, `source_file` (for local file sources), and
`ingestion_timestamp`. Silver and Gold contain the same validated mapped
schema. Quarantine rows include source record/child indexes, the original
source record, the mapped record, and rejection reasons.

## Optional Spark foundation

The regular Python/JSON pipeline does not import or require Spark. To install
the optional Spark/Delta dependencies:

```powershell
python -m pip install -r requirements-spark.txt
```

`framework/spark_session.py` provides a lazy Spark session factory configured
for Delta Lake. A Spark session is not started by `main.py`; the Spark execution
and Delta table backends are subsequent implementation phases. On Windows,
local Spark may still require Hadoop `winutils.exe` and `HADOOP_HOME`; this
project does not install or configure them, and the Python backend remains
available without them.

## Run

From the project directory, with Python 3.11:

```powershell
python -m pip install -r requirements.txt
python main.py
```

For an offline, reproducible demonstration:

```powershell
python scripts/generate_data.py --output-dir data/sample --customers 10 --carts 20 --seed 42
python main.py --config config/sample_pipeline_config.yaml
```

The generated `customers.json` and `carts.json` contain no expected-answer
labels. `expected_rejects.json` stores those labels separately. The sample
configuration writes to `data/sample_output/` rather than replacing the live
API Gold outputs. Increase `--customers` and `--carts` to create larger input
files; generation streams JSON records to disk.

For a larger offline demonstration (100,000 customers and 100,000 carts):

```powershell
python scripts/generate_data.py --output-dir data/large_sample --customers 100000 --carts 100000 --seed 42
python main.py --config config/large_sample_pipeline_config.yaml
```

The large-sample configuration keeps its inputs, outputs, logs, and metrics
separate from both the quick sample and live API pipeline. Cart products flatten
into roughly 200,000 item rows (the exact count is determined by the fixed
random seed). The dashboard opens this large-sample configuration by default;
enter another config path in its sidebar to view a different pipeline.

### Run the dashboard

In PowerShell, open the DataFlow project folder and start Streamlit:

```powershell
Set-Location "C:\CandidateWorkspace\Open FDA\DataFlow"
python -m pip install -r requirements.txt
python -m streamlit run dashboard.py
```

The dependency installation is needed only once. Open
<http://localhost:8501> when Streamlit reports that the server is ready.
Stop the dashboard with `Ctrl+C` in the PowerShell window.

It displays the selected run and its previous run for the same dataset,
reconciliation checks, recorded pipeline failures, a Gold preview, and
quarantined records. It does not trigger or modify pipeline runs.

The checked sample run (seed 42, 10 customers, 20 carts) produced 7 valid
customers and 3 customer rejects; cart flattening produced 40 rows, with 33
valid Gold rows and 7 rejects. Both runs reconciled successfully.

An unsuccessful dataset is logged and does not prevent later enabled datasets
from running. The application exits with code 1 if any dataset fails.

Run tests and syntax checks with:

```powershell
python -m pytest -v
$frameworkFiles = Get-ChildItem framework -Filter '*.py' | ForEach-Object FullName
python -m py_compile main.py $frameworkFiles
python scripts/benchmark.py --records 100000 --seed 42
```

PowerShell does not expand `framework/*.py` for this Python command, so the
second command explicitly supplies the framework module paths.

## Configuration and extension

`config/pipeline_config.yaml` controls dataset enablement, source type and
location, mappings, required fields, primary keys, storage paths, logging,
metrics, field types, minimum values, and reference validation. Dataset
references are dependency-ordered automatically; a reference to a disabled or
failed dataset is an explicit pipeline failure. Existing transformation strategies are
`select_fields` and `flatten_nested_records`; adding datasets using these
strategies is configuration-only. Spark and Delta dependencies are not needed
for the default local JSON pipeline.

## Data dictionary and rules

| Entity / output | Grain | Fields and types | Key / rules |
|---|---|---|---|
| Customer source | One API user | `id` integer, `username` string, `email` string | `id` maps to `customer_id`; customer ID is unique; all mapped fields are required and non-null |
| `customers` Gold | One customer | `customer_id` integer, `username` string, `email` string | Primary key: `customer_id`; source duplicate after the first occurrence is quarantined |
| Cart source | One cart with nested products | `id`, `userId` integers; `date` ISO-8601 datetime; `products` array | `userId` references a customer |
| `cart_items` Silver/Gold | One product line per cart | `cart_id`, `user_id`, `product_id`, `quantity` integers; `cart_date` ISO-8601 datetime | Composite key: (`cart_id`, `product_id`); quantity must be at least 1; `user_id` must exist in the customers dataset |

The cart-to-customer relationship is validated against customers successfully
processed in the same execution. Silver and Gold outputs use snake_case names and
do not include internal lineage columns. Quarantine preserves run ID, source
record index, nested child index (when applicable), raw source data, and
validation reasons. API records do not have a source filename; file-based
Bronze records include `source_file`.

## Retry, restart, backfill, and history

- **Retry/restart:** rerun the same command. Gold, Silver, and quarantine are
  atomically replaced snapshots, so reruns do not append duplicate output
  rows. Each attempt receives a new run ID and metrics file. There is no
  automatic network retry or checkpoint resume.
- **Backfill:** configure the dataset source as a JSON/CSV file containing the
  backfill input and rerun. The current Gold snapshot is replaced; prior Gold
  snapshots are not versioned.
- **History:** Bronze keeps one immutable source snapshot per run. This records
  extraction history, not CDC/change history; Silver and Gold represent the
  latest successful snapshot.
- **Partial publication:** atomic replace applies per file. A process failure
  between layer writes can leave a new Bronze/Silver file with the old Gold
  snapshot; metrics report the failed run.

## Performance and trade-off

Run `python scripts/benchmark.py --records 100000 --seed 42` to measure
synthetic customer generation, transformation, and validation on the local
machine. The report includes record counts, elapsed time, and records/second;
timings are environment-dependent and are not treated as a guaranteed
throughput target. Duplicate and reference checks use hash sets for expected
linear-time membership checks instead of repeatedly scanning all prior rows.
One measured Windows/Python 3.11 run generated 100,000 rows in 0.0535 seconds
and transformed/validated them in 0.1980 seconds (about 505,142 rows/second).

## Limitations

- Filesystem publishing is atomic per file, not transactional across all
  medallion layers.
- The default API extractor is synchronous and does not implement retries or
  incremental/watermark extraction. Bronze snapshots preserve run-level input
  history, but source change history is not inferred.
- JSON is the configured storage format; CSV writing remains available through
  `DataLoader` but is not used by the medallion pipeline.