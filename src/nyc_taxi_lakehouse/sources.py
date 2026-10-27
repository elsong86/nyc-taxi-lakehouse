import csv
import hashlib
import json
import logging
import re
import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from nyc_taxi_lakehouse.config import parse_month

LOGGER = logging.getLogger(__name__)
INDEX_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
SOURCE_ROOT = "https://d37ci6vzurychx.cloudfront.net"
ZONE_URL = f"{SOURCE_ROOT}/misc/taxi_zone_lookup.csv"


def session() -> requests.Session:
    client = requests.Session()
    client.headers["User-Agent"] = "Mozilla/5.0 (compatible; NYCTaxiLakehouse/0.1)"
    client.headers["Accept-Encoding"] = "identity"
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
    )
    client.mount("https://", HTTPAdapter(max_retries=retries))
    return client


def published_months(html: str, start: str, end: str) -> list[str]:
    parse_month(start)
    if end != "latest":
        parse_month(end)
    months = sorted(set(re.findall(r"yellow_tripdata_(\d{4}-\d{2})\.parquet", html)))
    if not months:
        raise ValueError("TLC index contains no yellow taxi files; source discovery failed.")
    for month in months:
        parse_month(month)
    selected = [m for m in months if m >= start and (end == "latest" or m <= end)]
    if not selected:
        raise ValueError(f"No published files in requested range {start}..{end}.")
    last = selected[-1] if end == "latest" else end
    expected = []
    current = parse_month(start)
    while current <= parse_month(last):
        expected.append(current.strftime("%Y-%m"))
        current = date_next_month(current)
    missing = sorted(set(expected) - set(selected))
    if missing:
        raise ValueError(f"Requested TLC months are missing from the source index: {missing}")
    return selected


def date_next_month(value):
    return value.replace(year=value.year + (value.month == 12), month=value.month % 12 + 1)


def discover(client: requests.Session, start: str, end: str) -> list[str]:
    response = client.get(INDEX_URL, timeout=(15, 90))
    response.raise_for_status()
    return published_months(response.text, start, end)


def trip_url(month: str) -> str:
    parse_month(month)
    return f"{SOURCE_ROOT}/trip-data/yellow_tripdata_{month}.parquet"


def remote_metadata(client: requests.Session, url: str) -> dict:
    response = client.head(url, timeout=(15, 90), allow_redirects=True)
    response.raise_for_status()
    headers = response.headers
    length = headers.get("Content-Length")
    response.close()
    if length is None:
        LOGGER.info("HEAD omitted Content-Length; requesting range metadata: %s", url)
        with client.get(
            url, headers={"Range": "bytes=0-0"}, stream=True, timeout=(15, 90)
        ) as ranged:
            ranged.raise_for_status()
            headers = ranged.headers
            if ranged.status_code == 206:
                match = re.fullmatch(r"bytes \d+-\d+/(\d+)", headers.get("Content-Range", ""))
                if match is None:
                    raise ValueError(f"Invalid Content-Range from {url}")
                length = match.group(1)
            elif ranged.status_code == 200:
                length = headers.get("Content-Length")
                if length is None:
                    length = sum(len(chunk) for chunk in ranged.iter_content(1024 * 1024))
            else:
                raise ValueError(f"Unexpected metadata response {ranged.status_code} from {url}")
    length = int(length)
    if length <= 0:
        raise ValueError(f"Empty source: {url}")
    return {
        "url": url,
        "content_length": length,
        "etag": headers.get("ETag", ""),
        "last_modified": headers.get("Last-Modified", ""),
    }


def check_revision(record: dict, remote: dict) -> None:
    for key in ("url", "content_length", "etag", "last_modified"):
        if record[key] != remote[key]:
            raise ValueError(
                f"Source changed ({key}): {remote['url']}; review before replacing raw data."
            )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_download(path: Path, expected_bytes: int) -> None:
    if path.stat().st_size != expected_bytes:
        raise ValueError(f"Incomplete download: {path.name}")
    if path.suffix == ".parquet":
        if expected_bytes < 8:
            raise ValueError(f"Invalid Parquet header/footer: {path.name}")
        with path.open("rb") as stream:
            first = stream.read(4)
            stream.seek(-4, 2)
            last = stream.read(4)
        if first != b"PAR1" or last != b"PAR1":
            raise ValueError(f"Invalid Parquet header/footer: {path.name}")
    elif path.suffix == ".csv":
        with path.open(newline="") as stream:
            header = csv.DictReader(stream).fieldnames or []
        if not {"LocationID", "Borough", "Zone", "service_zone"}.issubset(header):
            raise ValueError(f"Unexpected taxi zone CSV schema: {header}")


def land_file(client: requests.Session, destination: Path, remote: dict) -> tuple[dict, int]:
    manifest_path = destination.with_suffix(destination.suffix + ".manifest.json")
    if manifest_path.exists():
        try:
            record = json.loads(manifest_path.read_text())
        except json.JSONDecodeError:
            LOGGER.warning("Repairing interrupted landing manifest: %s", manifest_path)
        else:
            check_revision(record, remote)
            validate_download(destination, record["content_length"])
            if sha256_file(destination) != record["sha256"]:
                raise ValueError(f"Landed file checksum mismatch: {destination}")
            return record, 0

    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="nyc-taxi-") as directory:
        temporary = Path(directory) / destination.name
        with client.get(remote["url"], stream=True, timeout=(15, 120)) as response:
            response.raise_for_status()
            for key, header in (("etag", "ETag"), ("last_modified", "Last-Modified")):
                if response.headers.get(header, "") != remote[key]:
                    raise ValueError(f"Source changed during download ({key}): {remote['url']}")
            with temporary.open("wb") as stream:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    stream.write(chunk)
        validate_download(temporary, remote["content_length"])
        record = {
            **remote,
            "sha256": sha256_file(temporary),
            "downloaded_at": datetime.now(UTC).isoformat(),
        }
        # A manifest is the commit marker: an interrupted copy can be repaired on rerun.
        shutil.copyfile(temporary, destination)
        if sha256_file(destination) != record["sha256"]:
            raise ValueError(f"Upload checksum mismatch: {destination}")
        manifest_path.write_text(json.dumps(record, indent=2))
    LOGGER.info("Landed %s (%s bytes)", destination.name, remote["content_length"])
    return record, remote["content_length"]


def select_months(months: list[str], completed: set[str], limit: int, reprocess: bool) -> list[str]:
    return [month for month in months if reprocess or month not in completed][:limit]
