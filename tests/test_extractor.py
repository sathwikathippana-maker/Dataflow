import json

from framework.extractor import DataExtractor


def test_extract_api_dispatch(monkeypatch):
    class Response:
        def raise_for_status(self):
            pass

        def json(self):
            return [{"id": 1}]

    calls = {}

    def fake_get(url, headers=None, timeout=None):
        calls.update({
            "url": url,
            "headers": headers,
            "timeout": timeout
        })
        return Response()

    monkeypatch.setattr("framework.extractor.requests.get", fake_get)

    records = DataExtractor().extract(
        "api",
        "https://example.test/items",
        timeout=7,
        headers={"X-Test": "value"}
    )

    assert records == [{"id": 1}]
    assert calls == {
        "url": "https://example.test/items",
        "headers": {"X-Test": "value"},
        "timeout": 7
    }


def test_extract_json_dispatch(tmp_path):
    input_path = tmp_path / "input.json"
    input_path.write_text(json.dumps([{"id": 1}]), encoding="utf-8")

    assert DataExtractor().extract("json", input_path) == [{"id": 1}]