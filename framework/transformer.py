
class DataTransformer:
    """
    Reusable transformation component.
    Works with Python records without requiring Spark.
    """

    def select_fields(self, records, field_mapping, include_lineage=False):
        """
        Select approved fields and rename them.

        field_mapping format:
        {
            "output_field": "source_field"
        }
        """

        transformed = []

        for record_index, record in enumerate(records, start=1):
            row = {
                output_field: record.get(source_field)
                for output_field, source_field
                in field_mapping.items()
            }

            if include_lineage:
                row["_dataflow_source_record_index"] = record_index
            transformed.append(row)

        return transformed

    def flatten_nested_records(
        self,
        records,
        nested_field,
        parent_mapping,
        child_mapping,
        include_lineage=False
    ):
        """
        Flatten a nested array into individual records.
        """

        transformed = []

        for record_index, record in enumerate(records, start=1):
            children = record.get(nested_field, [])

            if children is None:
                children = []

            if not isinstance(children, list):
                if not include_lineage:
                    raise ValueError(f"{nested_field} must contain a list")
                children = [None]
                parent_error = (
                    f"{nested_field} must contain a list"
                )
            else:
                parent_error = None

            for child_index, child in enumerate(children, start=1):
                if not isinstance(child, dict):
                    if not include_lineage:
                        raise ValueError(
                            f"Invalid nested record in {nested_field}"
                        )

                row = {
                    output_field: record.get(source_field)
                    for output_field, source_field
                    in parent_mapping.items()
                }

                row.update({
                    output_field: (
                        child.get(source_field)
                        if isinstance(child, dict)
                        else None
                    )
                    for output_field, source_field
                    in child_mapping.items()
                })

                if include_lineage:
                    row["_dataflow_source_record_index"] = record_index
                    row["_dataflow_source_child_index"] = (
                        child_index if parent_error is None else None
                    )
                    errors = []
                    if parent_error:
                        errors.append(parent_error)
                    elif not isinstance(child, dict):
                        errors.append(
                            f"Invalid nested record in {nested_field}"
                        )
                    if errors:
                        row["_dataflow_transform_errors"] = errors

                transformed.append(row)

        return transformed
