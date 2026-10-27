import hashlib
import json
from unittest.mock import MagicMock

import pytest

from nyc_taxi_lakehouse.config import Config
from nyc_taxi_lakehouse.sources import (
    land_file,
    published_months,
    remote_metadata,
    select_months,
    validate_download,
)


def test_published_range_requires_every_requested_month():
    html = " ".join(
        f"https://example/yellow_tripdata_{month}.parquet"
        for month in ["2023-12", "2024-01", "2024-02", "2024-03"]
    )
    assert published_months(html, "2024-01", "2024-02") == ["2024-01", "2024-02"]
    assert published_months(html, "2024-02", "latest") == ["2024-02", "2024-03"]
    with pytest.raises(ValueError, match="missing"):
        published_months(html.replace("2024-02", "2024-04"), "2024-01", "2024-03")
    with pytest.raises(ValueError, match="discovery failed"):
        published_months("<html>error page</html>", "2024-01", "latest")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"start_month": "2024-13"},
        {"start_month": "2024-1"},
        {"start_month": "2024-03", "end_month": "2024-01"},
        {"max_new_months": 0},
        {"catalog": "catalog; DROP TABLE trips"},
    ],
)
def test_invalid_config_fails_explicitly(kwargs):
    with pytest.raises(ValueError):
        Config(**kwargs)


def test_completed_months_are_skipped_and_reprocessing_is_bounded():
    months = ["2024-01", "2024-02", "2024-03"]
    assert select_months(months, {"2024-01"}, 1, False) == ["2024-02"]
    assert select_months(months, set(months), 3, False) == []
    assert select_months(months, set(months), 1, True) == ["2024-01"]


def test_missing_head_length_uses_content_range_not_range_body_size():
    client = MagicMock()
    client.head.return_value.headers = {}
    ranged = client.get.return_value.__enter__.return_value
    ranged.status_code = 206
    ranged.headers = {"Content-Length": "1", "Content-Range": "bytes 0-0/12331", "ETag": "one"}
    record = remote_metadata(client, "https://example/zones.csv")
    assert record["content_length"] == 12331
    assert record["etag"] == "one"


def test_missing_length_with_ignored_range_counts_actual_response_bytes():
    client = MagicMock()
    client.head.return_value.headers = {}
    ranged = client.get.return_value.__enter__.return_value
    ranged.status_code = 200
    ranged.headers = {}
    ranged.iter_content.return_value = [b"abc", b"def"]
    assert remote_metadata(client, "https://example/zones.csv")["content_length"] == 6


def download_client(payload):
    client = MagicMock()
    response = client.get.return_value.__enter__.return_value
    response.iter_content.return_value = [payload]
    response.headers = {"ETag": '"version-one"', "Last-Modified": "yesterday"}
    return client


def metadata(payload):
    return {
        "url": "https://example/yellow_tripdata_2024-01.parquet",
        "content_length": len(payload),
        "etag": '"version-one"',
        "last_modified": "yesterday",
    }


def test_landing_is_checksum_verified_and_replay_does_not_download(tmp_path):
    payload = b"PAR1samplePAR1"
    destination = tmp_path / "yellow_tripdata_2024-01.parquet"
    client = download_client(payload)
    record, downloaded = land_file(client, destination, metadata(payload))
    assert downloaded == len(payload)
    assert record["sha256"] == hashlib.sha256(payload).hexdigest()
    assert destination.read_bytes() == payload
    client.reset_mock()
    assert land_file(client, destination, metadata(payload)) == (record, 0)
    client.get.assert_not_called()
    with pytest.raises(ValueError, match="Source changed"):
        land_file(client, destination, {**metadata(payload), "etag": '"version-two"'})
    destination.write_bytes(b"PAR1tamperPAR1")
    with pytest.raises(ValueError, match="checksum mismatch"):
        land_file(client, destination, metadata(payload))


def test_interrupted_uncommitted_copy_is_repaired(tmp_path):
    payload = b"PAR1completePAR1"
    destination = tmp_path / "yellow_tripdata_2024-01.parquet"
    destination.write_bytes(b"partial")
    record, _ = land_file(download_client(payload), destination, metadata(payload))
    assert destination.read_bytes() == payload
    manifest = json.loads(destination.with_suffix(".parquet.manifest.json").read_text())
    assert manifest["sha256"] == record["sha256"]


def test_interrupted_manifest_is_repaired_with_a_warning(tmp_path, caplog):
    payload = b"PAR1completePAR1"
    destination = tmp_path / "yellow_tripdata_2024-01.parquet"
    destination.write_bytes(payload)
    destination.with_suffix(".parquet.manifest.json").write_text('{"url":')
    land_file(download_client(payload), destination, metadata(payload))
    assert "Repairing interrupted landing manifest" in caplog.text
    assert json.loads(destination.with_suffix(".parquet.manifest.json").read_text())["sha256"]


def test_revision_between_head_and_get_is_rejected(tmp_path):
    payload = b"PAR1samplePAR1"
    client = download_client(payload)
    client.get.return_value.__enter__.return_value.headers["ETag"] = '"changed"'
    destination = tmp_path / "yellow_tripdata_2024-01.parquet"
    with pytest.raises(ValueError, match="Source changed during download"):
        land_file(client, destination, metadata(payload))
    assert not destination.exists()


def test_invalid_download_does_not_publish_a_manifest(tmp_path):
    payload = b"not parquet"
    destination = tmp_path / "yellow_tripdata_2024-01.parquet"
    with pytest.raises(ValueError, match="Invalid Parquet"):
        land_file(download_client(payload), destination, metadata(payload))
    assert not destination.exists()
    assert not destination.with_suffix(".parquet.manifest.json").exists()
    destination.write_bytes(b"PAR1short")
    with pytest.raises(ValueError, match="Incomplete"):
        validate_download(destination, 100)
