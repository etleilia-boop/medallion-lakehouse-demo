"""Paths and table layout for the lakehouse."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

# Open-ended validity for current SCD2 rows.
HIGH_DATE = "9999-12-31 00:00:00"


@dataclass(frozen=True)
class LakehouseConfig:
    """Where each layer lives. Everything derives from a single root folder."""

    root: Path
    table_format: str = "parquet"  # "parquet" (local/CI) or "delta" (Databricks/Synapse)

    @property
    def landing(self) -> Path:
        return self.root / "landing"

    @property
    def bronze(self) -> Path:
        return self.root / "lake" / "bronze"

    @property
    def silver(self) -> Path:
        return self.root / "lake" / "silver"

    @property
    def gold(self) -> Path:
        return self.root / "lake" / "gold"

    @property
    def quarantine(self) -> Path:
        return self.root / "lake" / "quarantine"

    def table(self, layer: str, name: str) -> str:
        return str(getattr(self, layer) / name)
