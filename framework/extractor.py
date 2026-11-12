
import csv
import json
import logging
from pathlib import Path

import requests


logger = logging.getLogger(__name__)


class DataExtractor:
    """
    Reusable extraction component for REST APIs,
    CSV files, and JSON files.

    Returns Python records. Spark conversion happens
    in a separate processing component.
    """

    def extract(
        self,
        source_type,
        source,
        records_key=None,
        timeout=30,
        headers=None
    ):
        """Dispatch extraction using the configured source type."""
        source_type = source_type.lower()

        if source_type == "api":
            return self.extract_api(
                source,
                records_key=records_key,
                timeout=timeout,
                headers=headers
            )
        if source_type == "csv":
            return self.extract_csv(source)
        if source_type == "json":
            return self.extract_json(source)

        raise ValueError(f"Unsupported source type: {source_type}")

    def extract_api(
        self,
        url,
        records_key=None,
        timeout=30,
        headers=None
    ):
        logger.info("Starting API extraction: %s", url)

        try:
            response = requests.get(
                url,
                headers=headers,
                timeout=timeout
            )
            response.raise_for_status()

            payload = response.json()

            if records_key:
                if not isinstance(payload, dict):
                    raise ValueError("Expected a JSON object")

                if records_key not in payload:
                    raise ValueError(
                        f"Key '{records_key}' not found"
                    )

                records = payload[records_key]

            elif isinstance(payload, list):
                records = payload

            elif isinstance(payload, dict):
                records = [payload]

            else:
                raise ValueError("Unsupported API response")

            if not isinstance(records, list):
                raise ValueError("Records must be a list")

            if not all(isinstance(r, dict) for r in records):
                raise ValueError("Every record must be an object")

            logger.info("API extraction completed: %s records", len(records))

            return records

        except Exception:
            logger.exception("API extraction failed")
            raise

    def extract_csv(self, file_path):
        path = Path(file_path)

        if not path.is_file():
            raise FileNotFoundError(file_path)

        with path.open("r", encoding="utf-8-sig", newline="") as file:
            return list(csv.DictReader(file))

    def extract_json(self, file_path):
        path = Path(file_path)

        if not path.is_file():
            raise FileNotFoundError(file_path)

        with path.open("r", encoding="utf-8") as file:
            payload = json.load(file)

        if isinstance(payload, list):
            return payload

        if isinstance(payload, dict):
            return [payload]

        raise ValueError("Unsupported JSON file format")
