"""Result records, timing helpers, and persistence."""
from __future__ import annotations

import csv
import json
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from statistics import median

OK = "ok"
UNSUPPORTED = "unsupported"
FAILED = "failed"


@dataclass
class StepResult:
    query_engine: str        # read engine for queries (e.g. "starrocks-4.1.1", "spark");
                             # "writer-spark" for load/compact rows
    candidate: str           # iceberg scenario, e.g. "iceberg-v3-mor"
    compaction: str          # compaction mode: none | every_round | every_2_rounds
    round: int
    phase: str               # 'load' | 'compact' | 'query'
    status: str              # ok | unsupported | failed
    duration_s: float = 0.0
    rows: int = 0
    error: str = ""
    extra: dict = field(default_factory=dict)


@contextmanager
def timed():
    """Yields a one-element list; on exit element[0] holds elapsed seconds."""
    holder = [0.0]
    start = time.perf_counter()
    try:
        yield holder
    finally:
        holder[0] = time.perf_counter() - start


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    if len(s) == 1:
        return s[0]
    k = (len(s) - 1) * pct
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def summarize_query(durations: list[float]) -> dict:
    return {
        "p50": median(durations) if durations else 0.0,
        "p95": percentile(durations, 0.95),
        "min": min(durations) if durations else 0.0,
        "max": max(durations) if durations else 0.0,
        "n": len(durations),
    }


class ResultSink:
    """Append-only persistence under results/<timestamp>/."""

    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.jsonl = self.root / "results.jsonl"
        self.csv = self.root / "results.csv"
        self._results: list[StepResult] = []

    def record(self, r: StepResult) -> None:
        self._results.append(r)
        with self.jsonl.open("a") as fh:
            fh.write(json.dumps(asdict(r)) + "\n")
        tag = r.status.upper()
        print(f"  [{tag:11}] {r.candidate}/{r.compaction}/{r.query_engine} round={r.round} "
              f"{r.phase}={r.duration_s:.3f}s rows={r.rows}"
              + (f" err={r.error[:110]}" if r.error else ""))

    @property
    def results(self) -> list[StepResult]:
        return self._results

    def flush_csv(self) -> None:
        cols = ["candidate", "compaction", "query_engine", "round", "phase", "status",
                "duration_s", "rows", "error"]
        with self.csv.open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=cols)
            w.writeheader()
            for r in self._results:
                d = asdict(r)
                w.writerow({k: d[k] for k in cols})

    def write_manifest(self, manifest: dict) -> None:
        (self.root / "manifest.json").write_text(json.dumps(manifest, indent=2))
