"""Benchmark orchestrator.

For every (Iceberg scenario × compaction mode), Spark writes the table round by round;
compaction runs per the cadence; then EACH read engine (StarRocks, Spark) queries the
last-2-rounds. Random data is pre-staged (datagen) and never timed here.

This single pass covers both "sessions":
  - read=StarRocks (session 1)   - read=Spark (session 2, can read v3 deletion vectors)
"""
from __future__ import annotations

import time

from . import datagen
from .adapters import AdapterContext, UnsupportedOperation, get_adapter
from .config import BenchConfig, Candidate
from .engines.polaris_client import PolarisClient
from .engines.spark_client import SparkClient
from .engines.starrocks_client import StarRocksClient
from .metrics import (FAILED, OK, UNSUPPORTED, ResultSink, StepResult,
                      summarize_query, timed)
from .schema import build_columns

WRITER = "writer-spark"   # query_engine tag for load/compact rows


def _recent_round_ids(r: int, k: int) -> list[int]:
    return list(range(max(0, r - k + 1), r + 1))


def _compaction_due(mode: str, r: int) -> bool:
    if mode == "every_round":
        return r >= 1
    if mode == "every_10_rounds":
        return r >= 1 and r % 10 == 0
    return False  # none


class Runner:
    def __init__(self, cfg: BenchConfig, sink: ResultSink):
        self.cfg = cfg
        self.sink = sink
        self.sr = StarRocksClient(cfg)
        self.spark = SparkClient(cfg)
        self.polaris = PolarisClient(cfg)
        self.columns = build_columns(cfg.schema)
        # read engines as (label, kind)
        self.read_engines = []
        for e in cfg.read_engines:
            if e == "starrocks":
                self.read_engines.append((cfg.query_engine, "starrocks"))
            elif e == "spark":
                self.read_engines.append(("spark", "spark"))

    def run(self, only_candidate: str | None = None,
            only_compaction: str | None = None) -> None:
        manifest = datagen.load_manifest()
        expected = {m["round"]: m.get("expected_recent") for m in manifest["rounds"]}
        engines = ", ".join(lbl for lbl, _ in self.read_engines)
        print(f"\n=== writer: spark | read engines: {engines} ===")
        for cname, candidate in self.cfg.candidates.items():
            if only_candidate and cname != only_candidate:
                continue
            for mode in self.cfg.compaction_modes:
                if only_compaction and mode != only_compaction:
                    continue
                self.run_one(candidate, mode, expected)
        self.sink.flush_csv()

    # ---- one (candidate × compaction mode) -----------------------------------
    def run_one(self, candidate: Candidate, mode: str, expected: dict) -> None:
        print(f"\n--- {candidate.name} (v{candidate.iceberg_format_version}/{candidate.mode}) "
              f"| compaction={mode} ---")
        ctx = AdapterContext(cfg=self.cfg, columns=self.columns, sr=self.sr,
                             flink=None, spark=self.spark, polaris=self.polaris)
        adapter = get_adapter(candidate.adapter)(ctx, candidate)
        wl = self.cfg.workload

        try:
            adapter.prepare()
        except Exception as e:  # noqa: BLE001
            self._record_all(candidate, mode, wl.rounds, FAILED, f"prepare: {e}")
            self._safe_cleanup(adapter)
            return

        try:
            self._do_load(adapter, candidate, mode, 0)        # seed
            for r in range(1, wl.rounds + 1):
                if not self._do_load(adapter, candidate, mode, r):
                    continue
                if _compaction_due(mode, r):
                    self._do_compact(adapter, candidate, mode, r)
                    self._do_maintain(adapter, candidate, mode, r)
                for label, kind in self.read_engines:
                    # Skip engines a candidate can't be read by (e.g. StarRocks cannot read
                    # Iceberg v3-MOR deletion vectors): capabilities `{kind}_read: false`.
                    if not candidate.capabilities.get(f"{kind}_read", True):
                        continue
                    # write->read freshness probe first (visibility lag), then steady-state query
                    self._do_freshness(adapter, candidate, mode, r, label, kind, expected.get(r))
                    self._do_query(adapter, candidate, mode, r, label, kind, expected.get(r))
        finally:
            self._safe_cleanup(adapter)

    # ---- steps ----------------------------------------------------------------
    def _quiesce(self) -> None:
        """Idle settle before a measured timer (called OUTSIDE timed()). Lets the prior
        step's residual IO/commit/GC drain here instead of bleeding into the measurement
        window — the main defense against one-off latency spikes. settle_s=0 is a no-op."""
        if self.cfg.settle_s > 0:
            time.sleep(self.cfg.settle_s)

    def _do_load(self, adapter, candidate, mode, r) -> bool:
        s3_uri = self.cfg.staging_s3_uri(f"round_{r:02d}.parquet")
        try:
            self._quiesce()
            with timed() as t:
                rows = adapter.load_round(r, s3_uri)
            self._rec(candidate, mode, WRITER, r, "load", OK, t[0], rows,
                      extra=adapter.snapshot_summary())  # write-amplification metrics (untimed)
            return True
        except UnsupportedOperation as e:
            self._rec(candidate, mode, WRITER, r, "load", UNSUPPORTED, error=str(e)); return False
        except Exception as e:  # noqa: BLE001
            self._rec(candidate, mode, WRITER, r, "load", FAILED, error=str(e)); return False

    def _do_compact(self, adapter, candidate, mode, r) -> None:
        try:
            self._quiesce()
            with timed() as t:
                summary = adapter.compact()
            self._rec(candidate, mode, WRITER, r, "compact", OK, t[0],
                      extra={**summary, **adapter.snapshot_summary()})
        except Exception as e:  # noqa: BLE001
            self._rec(candidate, mode, WRITER, r, "compact", FAILED, error=str(e))

    def _do_maintain(self, adapter, candidate, mode, r) -> None:
        try:
            self._quiesce()
            with timed() as t:
                summary = adapter.maintain()
            self._rec(candidate, mode, WRITER, r, "maintain", OK, t[0], extra=summary)
        except Exception as e:  # noqa: BLE001
            self._rec(candidate, mode, WRITER, r, "maintain", FAILED, error=str(e))

    def _do_freshness(self, adapter, candidate, mode, r, label, kind, expected_rows) -> None:
        """Write->read visibility lag: time from commit until the just-written round `r`
        becomes readable. Uses a lightweight existence probe (is_round_visible, LIMIT 1) so the
        metric isolates visibility lag from full read-execution cost (the steady-state read is
        measured separately by _do_query). Engine errors (e.g. v3 deletion-vector unreadable)
        are recorded immediately as failed (visibility N/A)."""
        self._quiesce()
        t0 = time.perf_counter()
        deadline = t0 + self.cfg.freshness_timeout_s
        while True:
            try:
                adapter.before_query(kind)
                visible = adapter.is_round_visible(r, kind)
            except UnsupportedOperation as e:
                self._rec(candidate, mode, label, r, "freshness", UNSUPPORTED, error=str(e)); return
            except Exception as e:  # noqa: BLE001 - engine cannot read this table state
                self._rec(candidate, mode, label, r, "freshness", FAILED, error=str(e)); return
            if visible:
                self._rec(candidate, mode, label, r, "freshness", OK, time.perf_counter() - t0)
                return
            if time.perf_counter() >= deadline:
                self._rec(candidate, mode, label, r, "freshness", FAILED,
                          time.perf_counter() - t0,
                          error=f"round {r} not visible in {self.cfg.freshness_timeout_s}s")
                return
            time.sleep(self.cfg.freshness_poll_s)

    def _do_query(self, adapter, candidate, mode, r, label, kind, expected_rows) -> None:
        ids = _recent_round_ids(r, self.cfg.workload.query_recent_rounds)
        durations: list[float] = []
        rows = 0
        try:
            self._quiesce()
            for _ in range(self.cfg.workload.query_repeats):
                adapter.before_query(kind)
                with timed() as t:
                    rows = adapter.run_query(ids, kind)
                durations.append(t[0])
            stats = summarize_query(durations)
            self._rec(candidate, mode, label, r, "query", OK, stats["p50"], rows,
                      extra={"stats": stats, "round_ids": ids, "expected_rows": expected_rows,
                             "correct": (expected_rows is None or rows == expected_rows)})
        except UnsupportedOperation as e:
            self._rec(candidate, mode, label, r, "query", UNSUPPORTED, error=str(e))
        except Exception as e:  # noqa: BLE001
            self._rec(candidate, mode, label, r, "query", FAILED, error=str(e))

    # ---- recording ------------------------------------------------------------
    def _rec(self, candidate, mode, engine, r, phase, status,
             duration=0.0, rows=0, error="", extra=None):
        self.sink.record(StepResult(
            query_engine=engine, candidate=candidate.name, compaction=mode, round=r,
            phase=phase, status=status, duration_s=duration, rows=rows,
            error=error, extra=extra or {}))

    def _record_all(self, candidate, mode, rounds, status, err) -> None:
        for r in range(0, rounds + 1):
            self._rec(candidate, mode, WRITER, r, "load", status, error=err)

    def _safe_cleanup(self, adapter) -> None:
        try:
            adapter.cleanup()
        except Exception as e:  # noqa: BLE001
            print(f"  [cleanup warning] {e}")


def wait_for_stack(cfg: BenchConfig) -> None:
    sr = StarRocksClient(cfg)
    print("waiting for StarRocks...")
    sr.wait_ready(timeout_s=300)
    print(f"StarRocks ready ({cfg.query_engine}).")
    sp = SparkClient(cfg)
    try:
        sp.wait_ready(timeout_s=240)
        print("Spark Connect ready.")
    except Exception as e:  # noqa: BLE001
        print(f"WARN: Spark not ready ({e}); loads will fail.")
    pc = PolarisClient(cfg)
    print(f"Polaris catalog present: {pc.catalog_exists()}")
