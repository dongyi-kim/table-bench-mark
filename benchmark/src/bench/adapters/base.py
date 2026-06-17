"""Adapter contract: one class per candidate variant.

A candidate is fully described by its YAML + an Adapter subclass. The runner owns
timing, query repeats, and result recording; adapters just *do the work* and may raise
UnsupportedOperation to have the runner record a clean `unsupported` result (e.g. when
StarRocks cannot write Iceberg v3).
"""
from __future__ import annotations

from dataclasses import dataclass

from ..config import BenchConfig, Candidate
from ..schema import Column

REGISTRY: dict[str, type["Adapter"]] = {}


def register(adapter_key: str):
    def deco(cls):
        REGISTRY[adapter_key] = cls
        cls.adapter_key = adapter_key
        return cls
    return deco


class UnsupportedOperation(RuntimeError):
    """Raised when a candidate genuinely cannot perform an operation (e.g. v3 write).
    The runner records status=unsupported rather than failing the whole run."""


@dataclass
class AdapterContext:
    cfg: BenchConfig
    columns: list[Column]
    sr: object          # StarRocksClient (always present; it is the query engine)
    flink: object | None  # FlinkClient (Session B only)
    spark: object | None = None   # SparkClient (Session A Iceberg writer)
    polaris: object = None  # PolarisClient
    db: str = "bench"


class Adapter:
    adapter_key: str = "base"

    def __init__(self, ctx: AdapterContext, candidate: Candidate):
        self.ctx = ctx
        self.cfg = ctx.cfg
        self.cols = ctx.columns
        self.candidate = candidate
        self.catalog_served: str = candidate.catalog  # may change (Paimon fallback)

    # ---- naming helpers -------------------------------------------------------
    @property
    def table(self) -> str:
        return self.candidate.name.replace("-", "_")

    @property
    def round_col(self) -> str:
        return self.cfg.schema.round_column

    @property
    def pk_col(self) -> str:
        return self.cfg.schema.pk_column

    def supports_upsert(self) -> bool:
        return bool(self.candidate.capabilities.get("upsert", True))

    # ---- lifecycle (override in subclasses) -----------------------------------
    def prepare(self) -> None:
        """Create catalog/db/table. Not timed."""
        raise NotImplementedError

    def load_round(self, round_idx: int, s3_uri: str) -> int:
        """Upsert one round from the staging Parquet at s3_uri. Return rows loaded.
        Runner wraps this in a timer. Raise UnsupportedOperation if impossible."""
        raise NotImplementedError

    def compact(self) -> dict:
        """Run compaction (e.g. Spark rewrite_data_files); return a small summary. Timed."""
        return {}

    def maintain(self) -> dict:
        """Snapshot/orphan maintenance (expire to 1 + remove orphans); summary. Timed separately."""
        return {}

    def before_query(self, engine: str) -> None:
        """Optional hook to refresh metadata so reads never see stale cache."""

    def run_query(self, round_ids: list[int], engine: str) -> int:
        """Run the 'last N rounds' query on the given read engine; return row count. Timed."""
        raise NotImplementedError

    def cleanup(self) -> None:
        """Drop the table so the next candidate starts fresh."""
