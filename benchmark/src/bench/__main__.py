"""CLI: python -m bench <command> [options]

commands:
  wait      block until StarRocks/Spark/Polaris are ready
  gen       pre-materialize seeded Parquet into staging/ + MinIO (never timed)
  smoke     tiny end-to-end run for every Iceberg scenario (sanity)
  bench     full benchmark run for the current StarRocks query engine
  report    build the Markdown comparison report from a results dir
  aggregate average N run dirs into one (per combo/round/phase) + report

The StarRocks version is swapped by scripts/run.sh, which calls `bench`/`smoke` once per
version with a shared --results-dir so both versions accumulate into one report.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from . import datagen
from .config import load_config
from .metrics import ResultSink
from .report import build_report
from .runner import Runner, wait_for_stack

RESULTS_ROOT = Path("/results")
STAGING = Path("/staging")


def _latest_results() -> Path:
    dirs = sorted([p for p in RESULTS_ROOT.iterdir() if p.is_dir()], reverse=True)
    if not dirs:
        sys.exit("no results found; run `bench` first")
    return dirs[0]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="bench")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("wait")
    g = sub.add_parser("gen")
    g.add_argument("--no-upload", action="store_true", help="local only, skip MinIO upload")
    g.add_argument("--smoke", action="store_true", help="generate smoke-sized data")

    for name in ("smoke", "bench"):
        p = sub.add_parser(name)
        p.add_argument("--candidate", help="limit to a single Iceberg scenario")
        p.add_argument("--compaction", help="limit to a single compaction mode")
        p.add_argument("--query-engine", help="override the StarRocks version label")
        p.add_argument("--results-dir", help="append into this results dir")
        p.add_argument("--skip-gen", action="store_true", help="reuse existing staging data")
        p.add_argument("--no-report", action="store_true", help="skip report generation")

    rp = sub.add_parser("report")
    rp.add_argument("--results-dir", help="defaults to latest")

    ag = sub.add_parser("aggregate")
    ag.add_argument("--run-dirs", nargs="+", required=True,
                    help="result dirs to average (same workload/config)")
    ag.add_argument("--output-dir", required=True, help="averaged results dir to create")
    ag.add_argument("--no-report", action="store_true", help="skip report generation")

    args = ap.parse_args(argv)
    cfg = load_config()

    if args.cmd == "wait":
        wait_for_stack(cfg)
        return 0

    if args.cmd == "gen":
        if args.smoke:
            cfg = cfg.with_smoke()
        m = datagen.generate(cfg, STAGING, upload=not args.no_upload)
        print(f"generated {len(m['rounds'])} rounds into {STAGING} "
              f"(seed={m['seed']}, cols={m['schema']['total_columns']})")
        return 0

    if args.cmd in ("smoke", "bench"):
        if args.cmd == "smoke":
            cfg = cfg.with_smoke()
        if args.query_engine:
            cfg.query_engine = args.query_engine
        if not args.skip_gen:
            print("generating staging data (not timed)...")
            datagen.generate(cfg, STAGING, upload=True)
        if args.results_dir:
            results_dir = Path(args.results_dir)
        else:
            ts = datetime.now().strftime("%Y%m%d-%H%M%S")
            results_dir = RESULTS_ROOT / f"{args.cmd}-{ts}"
        sink = ResultSink(results_dir)
        sink.write_manifest(datagen.load_manifest())
        Runner(cfg, sink).run(only_candidate=args.candidate, only_compaction=args.compaction)
        if not args.no_report:
            build_report(results_dir)
        else:
            print(f"results appended to {results_dir}")
        return 0

    if args.cmd == "report":
        rd = Path(args.results_dir) if args.results_dir else _latest_results()
        build_report(rd)
        return 0

    if args.cmd == "aggregate":
        from .aggregator import aggregate_results
        out = Path(args.output_dir)
        aggregate_results([Path(d) for d in args.run_dirs], out)
        if not args.no_report:
            build_report(out)
        else:
            print(f"aggregated results in {out}")
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
