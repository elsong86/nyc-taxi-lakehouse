import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path


def parse_month(value: str) -> date:
    if not re.fullmatch(r"\d{4}-\d{2}", value):
        raise ValueError(f"Expected YYYY-MM, got {value!r}")
    return date.fromisoformat(f"{value}-01")


@dataclass(frozen=True)
class Config:
    catalog: str = "nyc_taxi_dev"
    start_month: str = "2024-01"
    end_month: str = "latest"
    max_new_months: int = 3
    reprocess: bool = False
    job_run_id: str = "local"

    def __post_init__(self):
        if not re.fullmatch(r"[a-z][a-z0-9_]*", self.catalog):
            raise ValueError("Catalog must be a lowercase SQL identifier.")
        start = parse_month(self.start_month)
        if self.end_month != "latest" and parse_month(self.end_month) < start:
            raise ValueError("end_month must not precede start_month.")
        if self.max_new_months < 1:
            raise ValueError("max_new_months must be positive.")

    @property
    def landing(self) -> Path:
        return Path(f"/Volumes/{self.catalog}/raw/landing")

    def trip_path(self, month: str) -> Path:
        parse_month(month)
        return self.landing / "yellow" / f"yellow_tripdata_{month}.parquet"

    def table(self, schema: str, name: str) -> str:
        return f"{self.catalog}.{schema}.{name}"
