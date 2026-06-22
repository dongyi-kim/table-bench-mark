"""Average multiple benchmark runs into one results dir that build_report can consume.

Groups records across runs by (candidate, compaction, query_engine, round, phase) and emits
ONE averaged record per group: mean of the OK durations, with run-count / std / cv / per-run
values stashed in `extra`. The existing build_report then treats the average like a single run
— per-round graphs show the cross-run mean, summary tables average across rounds — while the
variability in `extra` feeds the comparison report's CV columns.
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from statistics import mean, pstdev

from .metrics import ResultSink, StepResult


def _load(run_dir: Path) -> list[dict]:
    rows: list[dict] = []
    with (run_dir / "results.jsonl").open() as fh:
        for line in fh:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def aggregate_results(run_dirs: list[Path], output_dir: Path) -> int:
    """Write output_dir/results.jsonl as the per-(combo,round,phase) mean across run_dirs.
    Returns the number of averaged rows."""
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for rd in run_dirs:
        for r in _load(rd):
            key = (r["candidate"], r["compaction"], r["query_engine"], r["round"], r["phase"])
            groups[key].append(r)

    sink = ResultSink(output_dir)
    for (cand, comp, eng, rnd, phase), recs in groups.items():
        ok = [x for x in recs if x["status"] == "ok"]
        if ok:
            durs = [x["duration_s"] for x in ok]
            m = mean(durs)
            sd = pstdev(durs) if len(durs) > 1 else 0.0
            extra = {"runs": len(recs), "ok_runs": len(ok), "mean": m, "std": sd,
                     "cv": (sd / m if m else 0.0), "per_run": durs}
            stats = next((x.get("extra", {}).get("stats") for x in ok
                          if x.get("extra", {}).get("stats")), None)
            if stats:
                extra["stats"] = stats
            sink.record(StepResult(
                query_engine=eng, candidate=cand, compaction=comp, round=rnd, phase=phase,
                status="ok", duration_s=m, rows=ok[-1].get("rows", 0), extra=extra))
        else:
            # No OK in any run -> keep one representative status (unsupported preferred).
            rep = next((x for x in recs if x["status"] == "unsupported"), recs[0])
            sink.record(StepResult(
                query_engine=eng, candidate=cand, compaction=comp, round=rnd, phase=phase,
                status=rep["status"], duration_s=0.0, rows=0, error=rep.get("error", ""),
                extra={"runs": len(recs), "ok_runs": 0}))

    # Workload is identical across runs -> carry the first manifest for the report header.
    for rd in run_dirs:
        mf = rd / "manifest.json"
        if mf.exists():
            (output_dir / "manifest.json").write_text(mf.read_text())
            break
    sink.flush_csv()
    total = sum(len(v) for v in groups.values())
    print(f"aggregated {total} records from {len(run_dirs)} runs "
          f"into {len(groups)} averaged rows -> {output_dir}")
    return len(groups)
