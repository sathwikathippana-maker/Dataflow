import argparse
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _label(expected_rejects, dataset, record_index, reason):
    expected_rejects.append({
        "dataset": dataset,
        "record_index": record_index,
        "reason": reason
    })


def iter_customers(
    count,
    seed=42,
    expected_rejects=None,
    inject_edge_cases=True
):
    expected_rejects = expected_rejects if expected_rejects is not None else []

    for index in range(count):
        record = {
            "id": index + 1,
            "username": f"user_{index + 1:06d}",
            "email": f"user_{index + 1:06d}@example.test"
        }

        if inject_edge_cases and index == 1:
            record.pop("email")
            _label(expected_rejects, "customers", index + 1, "missing_email")
        elif inject_edge_cases and count >= 4 and index == 2:
            record["email"] = None
            _label(expected_rejects, "customers", index + 1, "null_email")

        if inject_edge_cases and count >= 3 and index == count - 1:
            record["id"] = 1
            _label(
                expected_rejects,
                "customers",
                index + 1,
                "duplicate_customer_id"
            )

        yield record


def iter_carts(
    count,
    customer_count,
    seed=42,
    expected_rejects=None,
    inject_edge_cases=True
):
    expected_rejects = expected_rejects if expected_rejects is not None else []
    rng = random.Random(seed)
    date_origin = datetime(2025, 1, 1, tzinfo=timezone.utc)
    customer_ids = list(range(1, customer_count + 1))
    if inject_edge_cases and customer_count >= 2:
        customer_ids.remove(2)
    if inject_edge_cases and customer_count >= 4:
        customer_ids.remove(3)
    if inject_edge_cases and customer_count >= 3:
        last_customer_id = customer_count
        if last_customer_id in customer_ids:
            customer_ids.remove(last_customer_id)

    for index in range(count):
        user_id = rng.choice(customer_ids) if customer_ids else 1
        product_count = rng.randint(1, 3)
        products = [
            {"productId": product_id, "quantity": rng.randint(1, 10)}
            for product_id in rng.sample(range(1, 1001), product_count)
        ]
        cart_date = (
            date_origin + timedelta(days=rng.randint(0, 60))
        ).isoformat().replace("+00:00", "Z")
        record = {
            "id": index + 1,
            "userId": user_id,
            "date": cart_date,
            "products": products
        }

        if inject_edge_cases and index == 1:
            record["userId"] = customer_count + 1
            _label(
                expected_rejects,
                "cart_items",
                index + 1,
                "invalid_customer_reference"
            )
        elif inject_edge_cases and count >= 3 and index == 2:
            record["products"] = "malformed"
            _label(
                expected_rejects,
                "cart_items",
                index + 1,
                "malformed_products"
            )
        elif inject_edge_cases and count >= 4 and index == 3:
            record["products"][0]["quantity"] = 0
            _label(
                expected_rejects,
                "cart_items",
                index + 1,
                "non_positive_quantity"
            )
        elif inject_edge_cases and count >= 5 and index == 4:
            duplicate_product = dict(record["products"][0])
            duplicate_product["quantity"] = 2
            record["products"].append(duplicate_product)
            _label(
                expected_rejects,
                "cart_items",
                index + 1,
                "duplicate_cart_product_key"
            )

        yield record


def write_json_array(path, records):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")

    try:
        with temp_path.open("w", encoding="utf-8", newline="") as file:
            file.write("[")
            first = True
            for record in records:
                if not first:
                    file.write(",")
                json.dump(record, file, ensure_ascii=False)
                first = False
            file.write("]")
            file.flush()
            os.fsync(file.fileno())
        os.replace(temp_path, path)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def generate(output_dir, customer_count=10, cart_count=20, seed=42):
    if customer_count < 0 or cart_count < 0:
        raise ValueError("Record counts cannot be negative")

    output_dir = Path(output_dir)
    expected_rejects = []
    write_json_array(
        output_dir / "customers.json",
        iter_customers(
            customer_count,
            seed=seed,
            expected_rejects=expected_rejects
        )
    )
    write_json_array(
        output_dir / "carts.json",
        iter_carts(
            cart_count,
            customer_count,
            seed=seed,
            expected_rejects=expected_rejects
        )
    )
    write_json_array(
        output_dir / "expected_rejects.json",
        expected_rejects
    )

    return {
        "customers": customer_count,
        "carts": cart_count,
        "expected_rejects": len(expected_rejects),
        "seed": seed,
        "output_dir": str(output_dir)
    }


def main():
    parser = argparse.ArgumentParser(
        description="Generate deterministic DataFlow sample datasets"
    )
    parser.add_argument("--output-dir", default="data/sample")
    parser.add_argument("--customers", type=int, default=10)
    parser.add_argument("--carts", type=int, default=20)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    try:
        summary = generate(
            args.output_dir,
            customer_count=args.customers,
            cart_count=args.carts,
            seed=args.seed
        )
    except ValueError as error:
        parser.error(str(error))

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()