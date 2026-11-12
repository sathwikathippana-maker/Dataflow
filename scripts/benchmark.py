import argparse
import json
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from framework.transformer import DataTransformer
from framework.validator import DataValidator
from scripts.generate_data import iter_customers


def benchmark(record_count, seed):
    generation_started = time.perf_counter()
    source_records = list(
        iter_customers(
            record_count,
            seed=seed,
            inject_edge_cases=False
        )
    )
    generation_seconds = time.perf_counter() - generation_started

    transformer = DataTransformer()
    validator = DataValidator()

    pipeline_started = time.perf_counter()
    transformed = transformer.select_fields(
        source_records,
        {
            "customer_id": "id",
            "username": "username",
            "email": "email"
        }
    )
    validation = validator.validate(
        transformed,
        required_fields=["customer_id", "username", "email"],
        primary_key=["customer_id"],
        field_types={
            "customer_id": "integer",
            "username": "string",
            "email": "string"
        }
    )
    pipeline_seconds = time.perf_counter() - pipeline_started

    return {
        "seed": seed,
        "source_records": len(source_records),
        "transformed_records": len(transformed),
        "valid_records": validation["valid_count"],
        "rejected_records": validation["rejected_count"],
        "generation_seconds": round(generation_seconds, 4),
        "transform_and_validation_seconds": round(pipeline_seconds, 4),
        "transform_and_validation_records_per_second": round(
            len(transformed) / pipeline_seconds
            if pipeline_seconds
            else 0,
            2
        )
    }


def main():
    parser = argparse.ArgumentParser(
        description="Measure local transformation and validation throughput"
    )
    parser.add_argument("--records", type=int, default=100000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    if args.records < 0:
        parser.error("--records cannot be negative")

    print(json.dumps(benchmark(args.records, args.seed), indent=2))


if __name__ == "__main__":
    main()
