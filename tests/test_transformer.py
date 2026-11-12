
from framework.transformer import DataTransformer


def test_select_customer_fields():
    transformer = DataTransformer()

    records = [
        {"id": 1, "username": "john", "email": "john@gmail.com", "password": "secret"},
        {"id": 2, "username": "alice", "email": "alice@gmail.com", "password": "secret"}
    ]

    result = transformer.select_fields(
        records,
        {
            "customer_id": "id",
            "username": "username",
            "email": "email"
        }
    )

    assert len(result) == 2
    assert result[0]["customer_id"] == 1
    assert "password" not in result[0]


def test_flatten_cart_products():
    transformer = DataTransformer()

    records = [
        {
            "id": 1,
            "userId": 10,
            "date": "2026-11-12",
            "products": [
                {"productId": 101, "quantity": 2},
                {"productId": 102, "quantity": 3}
            ]
        }
    ]

    result = transformer.flatten_nested_records(
        records,
        nested_field="products",
        parent_mapping={
            "cart_id": "id",
            "user_id": "userId",
            "cart_date": "date"
        },
        child_mapping={
            "product_id": "productId",
            "quantity": "quantity"
        }
    )

    assert len(result) == 2
    assert result[0]["cart_id"] == 1
    assert result[0]["product_id"] == 101
    assert result[1]["product_id"] == 102


def test_empty_nested_products():
    transformer = DataTransformer()

    records = [
        {
            "id": 1,
            "userId": 10,
            "date": "2026-11-12",
            "products": []
        }
    ]

    result = transformer.flatten_nested_records(
        records,
        nested_field="products",
        parent_mapping={
            "cart_id": "id",
            "user_id": "userId"
        },
        child_mapping={
            "product_id": "productId"
        }
    )

    assert result == []


def test_invalid_nested_structure():
    transformer = DataTransformer()

    records = [
        {
            "id": 1,
            "products": "invalid"
        }
    ]

    try:
        transformer.flatten_nested_records(
            records,
            nested_field="products",
            parent_mapping={"cart_id": "id"},
            child_mapping={"product_id": "productId"}
        )
        assert False, "Expected ValueError"
    except ValueError:
        assert True


def test_lineage_mode_quarantines_malformed_nested_records():
    transformer = DataTransformer()

    result = transformer.flatten_nested_records(
        [{"id": 8, "products": "invalid"}],
        nested_field="products",
        parent_mapping={"cart_id": "id"},
        child_mapping={"product_id": "productId"},
        include_lineage=True
    )

    assert result[0]["_dataflow_source_record_index"] == 1
    assert result[0]["_dataflow_transform_errors"] == [
        "products must contain a list"
    ]
