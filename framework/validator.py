
from datetime import date, datetime


class DataValidator:
    """Reusable validation component with composite key support."""

    def validate(
        self,
        records,
        required_fields,
        primary_key,
        check_nulls=True,
        check_duplicates=True,
        field_types=None,
        minimum_values=None,
        references=None
    ):

        valid_records = []
        rejected_records = []
        seen_keys = set()
        field_types = field_types or {}
        minimum_values = minimum_values or {}
        references = references or {}
        supported_types = {
            "string",
            "integer",
            "number",
            "boolean",
            "date",
            "datetime"
        }

        unsupported_types = set(field_types.values()) - supported_types
        if unsupported_types:
            raise ValueError(
                f"Unsupported field types: {sorted(unsupported_types)}"
            )

        # Support both single and composite primary keys
        key_fields = (
            primary_key
            if isinstance(primary_key, (list, tuple))
            else [primary_key]
        )

        for row_number, record in enumerate(records, start=1):
            errors = list(record.get("_dataflow_transform_errors", []))

            # Required field and null checks
            for field in required_fields:
                if field not in record:
                    errors.append(f"Missing or null field: {field}")

                elif record[field] is None and check_nulls:
                    errors.append(f"Missing or null field: {field}")

                elif (
                    check_nulls
                    and isinstance(record[field], str)
                    and not record[field].strip()
                ):
                    errors.append(f"Empty field: {field}")

            for field, expected_type in field_types.items():
                if field in record and record[field] is not None:
                    if not self._matches_type(record[field], expected_type):
                        errors.append(
                            f"Invalid type for {field}: expected "
                            f"{expected_type}, got "
                            f"{type(record[field]).__name__}"
                        )

            for field, minimum in minimum_values.items():
                value = record.get(field)
                if value is not None:
                    if (
                        isinstance(value, bool)
                        or not isinstance(value, (int, float))
                    ):
                        errors.append(
                            f"Invalid type for {field}: minimum check "
                            "requires a number"
                        )
                    elif value < minimum:
                        errors.append(
                            f"Value below minimum for {field}: {minimum}"
                        )

            for field, reference in references.items():
                values = reference["values"]
                value = record.get(field)
                try:
                    is_valid_reference = value in values
                except TypeError:
                    is_valid_reference = False
                if not is_valid_reference:
                    errors.append(
                        f"Invalid reference for {field}: {value} is not in "
                        f"{reference['dataset']}.{reference['reference_field']}"
                    )

            # Primary keys remain mandatory when general null checks are disabled.
            key_values = tuple(record.get(field) for field in key_fields)
            has_key = all(
                field in record
                and record[field] is not None
                and not (
                    isinstance(record[field], str)
                    and not record[field].strip()
                )
                for field in key_fields
            )

            if not has_key:
                for field in key_fields:
                    if field not in record or record[field] is None:
                        reason = f"Missing or null field: {field}"
                        if reason not in errors:
                            errors.append(reason)
                    elif (
                        isinstance(record[field], str)
                        and not record[field].strip()
                    ):
                        reason = f"Empty field: {field}"
                        if reason not in errors:
                            errors.append(reason)
            elif check_duplicates:
                try:
                    if key_values in seen_keys:
                        errors.append(
                            f"Duplicate primary key: {key_fields}={key_values}"
                        )
                    else:
                        seen_keys.add(key_values)
                except TypeError:
                    errors.append(
                        f"Unhashable primary key: {key_fields}={key_values}"
                    )

            if errors:
                rejected_records.append({
                    "row_number": row_number,
                    "record": record,
                    "errors": errors
                })
            else:
                valid_records.append(record)

        return {
            "valid_records": valid_records,
            "rejected_records": rejected_records,
            "input_count": len(records),
            "valid_count": len(valid_records),
            "rejected_count": len(rejected_records)
        }

    @staticmethod
    def _matches_type(value, expected_type):
        if expected_type == "string":
            return isinstance(value, str)
        if expected_type == "integer":
            return isinstance(value, int) and not isinstance(value, bool)
        if expected_type == "number":
            return (
                isinstance(value, (int, float))
                and not isinstance(value, bool)
            )
        if expected_type == "boolean":
            return isinstance(value, bool)
        if expected_type == "date":
            if not isinstance(value, str):
                return False
            try:
                date.fromisoformat(value)
                return True
            except ValueError:
                return False
        if expected_type == "datetime":
            if not isinstance(value, str):
                return False
            try:
                datetime.fromisoformat(value.replace("Z", "+00:00"))
                return True
            except ValueError:
                return False
        return False
