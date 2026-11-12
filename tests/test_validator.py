
from framework.validator import DataValidator


def test_valid_records():
    validator = DataValidator()

    records = [
        {"customer_id": 1, "username": "john"},
        {"customer_id": 2, "username": "alice"}
    ]

    result = validator.validate(
        records,
        required_fields=["customer_id", "username"],
        primary_key="customer_id"
    )

    assert result["input_count"] == 2
    assert result["valid_count"] == 2
    assert result["rejected_count"] == 0


def test_empty_input():
    validator = DataValidator()

    result = validator.validate(
        [],
        required_fields=["customer_id", "username"],
        primary_key="customer_id"
    )

    assert result["input_count"] == 0
    assert result["valid_count"] == 0
    assert result["rejected_count"] == 0


def test_null_required_field():
    validator = DataValidator()

    records = [
        {"customer_id": 1, "username": None}
    ]

    result = validator.validate(
        records,
        required_fields=["customer_id", "username"],
        primary_key="customer_id"
    )

    assert result["valid_count"] == 0
    assert result["rejected_count"] == 1


def test_missing_required_field():
    validator = DataValidator()

    result = validator.validate(
        [{"customer_id": 1}],
        required_fields=["customer_id", "email"],
        primary_key="customer_id"
    )

    assert result["valid_count"] == 0
    assert result["rejected_count"] == 1
    assert "Missing or null field: email" in result["rejected_records"][0]["errors"]


def test_duplicate_primary_key():
    validator = DataValidator()

    records = [
        {"customer_id": 1, "username": "john"},
        {"customer_id": 1, "username": "alice"}
    ]

    result = validator.validate(
        records,
        required_fields=["customer_id", "username"],
        primary_key="customer_id"
    )

    assert result["valid_count"] == 1
    assert result["rejected_count"] == 1


def test_composite_primary_key():
    validator = DataValidator()

    records = [
        {"cart_id": 1, "product_id": 1, "quantity": 4},
        {"cart_id": 1, "product_id": 2, "quantity": 1},
        {"cart_id": 1, "product_id": 1, "quantity": 9}
    ]

    result = validator.validate(
        records,
        required_fields=["cart_id", "product_id", "quantity"],
        primary_key=["cart_id", "product_id"]
    )

    assert result["valid_count"] == 2
    assert result["rejected_count"] == 1


def test_empty_string_field():
    validator = DataValidator()

    records = [
        {"customer_id": 1, "username": ""}
    ]

    result = validator.validate(
        records,
        required_fields=["customer_id", "username"],
        primary_key="customer_id"
    )

    assert result["valid_count"] == 0
    assert result["rejected_count"] == 1


def test_type_minimum_and_reference_rules():
    validator = DataValidator()

    result = validator.validate(
        [
            {"cart_id": 1, "user_id": 4, "quantity": 0},
            {"cart_id": "2", "user_id": 99, "quantity": 2},
            {"cart_id": 3, "user_id": 4, "quantity": 1}
        ],
        required_fields=["cart_id", "user_id", "quantity"],
        primary_key=["cart_id"],
        field_types={
            "cart_id": "integer",
            "user_id": "integer",
            "quantity": "integer"
        },
        minimum_values={"quantity": 1},
        references={
            "user_id": {
                "dataset": "customers",
                "reference_field": "customer_id",
                "values": {4}
            }
        }
    )

    assert result["valid_records"] == [
        {"cart_id": 3, "user_id": 4, "quantity": 1}
    ]
    assert "Value below minimum for quantity: 1" in result["rejected_records"][0]["errors"]
    assert any(
        error.startswith("Invalid type for cart_id")
        for error in result["rejected_records"][1]["errors"]
    )
    assert any(
        error.startswith("Invalid reference for user_id")
        for error in result["rejected_records"][1]["errors"]
    )
