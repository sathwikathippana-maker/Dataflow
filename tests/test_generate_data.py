import json
from pathlib import Path

from scripts.generate_data import generate


def test_generator_is_seeded_and_keeps_expected_labels_separate(tmp_path):
    output_dir = tmp_path / "sample"

    generate(output_dir, customer_count=6, cart_count=8, seed=17)
    first_customers = (output_dir / "customers.json").read_bytes()
    first_carts = (output_dir / "carts.json").read_bytes()
    first_labels = (output_dir / "expected_rejects.json").read_bytes()

    generate(output_dir, customer_count=6, cart_count=8, seed=17)

    assert (output_dir / "customers.json").read_bytes() == first_customers
    assert (output_dir / "carts.json").read_bytes() == first_carts
    assert (output_dir / "expected_rejects.json").read_bytes() == first_labels

    customers = json.loads(first_customers)
    carts = json.loads(first_carts)
    labels = json.loads(first_labels)
    assert len(customers) == 6
    assert len(carts) == 8
    assert "expected_rejects" not in customers[0]
    assert "expected_rejects" not in carts[0]
    assert {label["reason"] for label in labels} >= {
        "missing_email",
        "duplicate_customer_id",
        "invalid_customer_reference",
        "malformed_products",
        "non_positive_quantity",
        "duplicate_cart_product_key"
    }
    valid_customer_ids = {1, 4, 5}
    for cart in carts:
        if cart["id"] != 2:
            assert cart["userId"] in valid_customer_ids

    dates = [cart["date"] for cart in carts]
    assert any(first > second for first, second in zip(dates, dates[1:]))


def test_generator_supports_empty_batches(tmp_path):
    summary = generate(tmp_path / "empty", customer_count=0, cart_count=0)

    assert summary["customers"] == 0
    assert summary["carts"] == 0
    assert json.loads(
        (tmp_path / "empty" / "customers.json").read_text(encoding="utf-8")
    ) == []
