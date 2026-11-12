
import csv
import json
import logging
import os
import uuid

from pathlib import Path


logger = logging.getLogger(__name__)


class DataLoader:
    """Reusable local storage component with atomic publishing."""

    def _atomic_publish(self, path, writer):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        temp_path = path.with_name(
            f".{path.name}.{uuid.uuid4().hex}.tmp"
        )

        try:
            with temp_path.open("w", encoding="utf-8", newline="") as file:
                writer(file)
                file.flush()
                os.fsync(file.fileno())

            # Replace the final file only after writing succeeds.
            os.replace(temp_path, path)

            return str(path)

        finally:
            # Remove temporary file if publishing failed.
            if temp_path.exists():
                temp_path.unlink()

    def write_json(self, records, file_path):

        def writer(file):
            json.dump(
                records,
                file,
                indent=4,
                ensure_ascii=False,
                default=str
            )

        result = self._atomic_publish(file_path, writer)

        logger.info(
            "Saved %s records to %s",
            len(records),
            result
        )

        return result

    def write_csv(self, records, file_path):

        if not records:
            raise ValueError("Cannot write CSV without records")

        def writer(file):
            fieldnames = list(records[0].keys())

            csv_writer = csv.DictWriter(
                file,
                fieldnames=fieldnames,
                extrasaction="ignore"
            )

            csv_writer.writeheader()
            csv_writer.writerows(records)

        result = self._atomic_publish(file_path, writer)

        logger.info(
            "Saved %s records to %s",
            len(records),
            result
        )

        return result
