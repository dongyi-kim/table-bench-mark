"""Build a Korean comparison report (Markdown + PNG graphs) from results.jsonl.

Dimensions: Iceberg scenario (v2/v3 × cow/mor) × compaction mode (none/every_round/
every_2_rounds) × read engine (StarRocks, Spark). Writes a compatibility matrix, latency
tables, matplotlib graphs, and an auto commentary (incl. whether compaction makes a v3-MOR
table readable by StarRocks).
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from tabulate import tabulate  # noqa: E402

WRITER = "writer-spark"
SCEN_ORDER = ["iceberg-v2-cow", "iceberg-v2-mor", "iceberg-v3-cow", "iceberg-v3-mor"]
MODE_ORDER = ["none", "every_round", "every_2_rounds"]


def _load(results_dir: Path) -> list[dict]:
    rows = []
    with (results_dir / "results.jsonl").open() as fh:
        for line in fh:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def _mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


def _order(items, ref):
    return [x for x in ref if x in items] + sorted(i for i in items if i not in ref)


def build_report(results_dir: Path) -> Path:
    rows = _load(results_dir)
    read_engines = _order({r["query_engine"] for r in rows if r["phase"] == "query"}, [])
    scenarios = _order({r["candidate"] for r in rows}, SCEN_ORDER)
    modes = _order({r["compaction"] for r in rows}, MODE_ORDER)

    # ---- aggregates -----------------------------------------------------------
    q_ok = defaultdict(list)        # (scen,mode,engine) -> [p50...]
    q_status = defaultdict(set)     # (scen,mode,engine) -> {status}
    load_ok = defaultdict(list)     # (scen,mode) -> [load sec] round>=1
    comp_ok = defaultdict(list)     # (scen,mode) -> [compact sec]
    last_err = {}

    for r in rows:
        scen, mode = r["candidate"], r["compaction"]
        if r["phase"] == "query":
            key = (scen, mode, r["query_engine"])
            q_status[key].add(r["status"])
            if r["status"] == "ok":
                q_ok[key].append(r["duration_s"])
            elif r.get("error"):
                last_err[key] = r["error"]
        elif r["phase"] == "load" and r["round"] >= 1 and r["status"] == "ok":
            load_ok[(scen, mode)].append(r["duration_s"])
        elif r["phase"] == "compact" and r["status"] == "ok":
            comp_ok[(scen, mode)].append(r["duration_s"])

    def compat(scen, mode, eng):
        st = q_status.get((scen, mode, eng), set())
        if not st:
            return "-"
        if st == {"ok"}:
            return "✓"
        if "ok" in st:
            return "△"
        return "✗"

    # ---- 1) compatibility matrix ---------------------------------------------
    headers = ["시나리오", "compaction"] + read_engines
    compat_tbl = [[s, m] + [compat(s, m, e) for e in read_engines]
                  for s in scenarios for m in modes]
    compat_md = tabulate(compat_tbl, headers=headers, tablefmt="github")

    # ---- 2) query latency table (p50 mean, s) --------------------------------
    q_tbl = [[s, m] + [f"{_mean(q_ok[(s, m, e)]):.3f}" if q_ok[(s, m, e)] else "—"
                       for e in read_engines]
             for s in scenarios for m in modes]
    query_md = tabulate(q_tbl, headers=headers, tablefmt="github")

    # ---- 3) load + compaction cost table -------------------------------------
    lc_tbl = [[s, m,
               f"{_mean(load_ok[(s, m)]):.3f}" if load_ok[(s, m)] else "—",
               f"{_mean(comp_ok[(s, m)]):.3f}" if comp_ok[(s, m)] else "—",
               f"{sum(comp_ok[(s, m)]):.1f}" if comp_ok[(s, m)] else "0"]
              for s in scenarios for m in modes]
    lc_md = tabulate(lc_tbl, headers=["시나리오", "compaction", "적재 평균(s)",
                                      "compaction 평균(s)", "compaction 총합(s)"],
                     tablefmt="github")

    # ---- graphs ---------------------------------------------------------------
    imgs = _graphs(rows, results_dir, scenarios, modes, read_engines)

    # ---- commentary -----------------------------------------------------------
    notes = []
    # v3-mor readability via compaction (StarRocks)
    sr = next((e for e in read_engines if e.startswith("starrocks")), None)
    if sr and "iceberg-v3-mor" in scenarios:
        line = []
        for m in modes:
            line.append(f"{m}={compat('iceberg-v3-mor', m, sr)}")
        notes.append(f"- **v3-MOR × {sr}** 호환성(compaction별): " + ", ".join(line)
                     + "  → compaction으로 deletion vector를 제거하면 StarRocks 조회 가능 여부 확인")
    # fastest query per engine
    for e in read_engines:
        cands = [((s, m), _mean(q_ok[(s, m, e)])) for s in scenarios for m in modes
                 if q_ok[(s, m, e)]]
        if cands:
            (s, m), v = min(cands, key=lambda x: x[1])
            notes.append(f"- **{e}** 최저 조회 지연: `{s}` / {m} ({v:.3f}s)")

    out = results_dir / "report.md"
    out.write_text(
        "# table-bench-mark 결과 리포트 (compaction × 조회엔진)\n\n"
        f"결과 디렉터리: `{results_dir.name}`\n\n"
        "적재=**Spark**(Iceberg), 조회=**StarRocks**/**Spark**(같은 테이블을 두 엔진이 조회). "
        "compaction은 Spark `rewrite_data_files`(deletion vector/delete 적용). "
        "랜덤 생성은 사전 staging으로 측정 제외, Polaris 캐시 비활성.\n\n"
        "## 1. 호환성 매트릭스 (✓ 정상 / △ 부분 / ✗ 불가 / - 없음)\n\n" + compat_md
        + "\n\n## 2. 조회 지연 (p50 평균, 초)\n\n" + query_md
        + "\n\n## 3. 적재 · compaction 비용 (초)\n\n" + lc_md
        + "\n\n## 4. 그래프\n\n"
        + "".join(f"### {title}\n\n![{title}]({fn})\n\n" for title, fn in imgs)
        + "## 5. 해설\n\n" + "\n".join(notes) + "\n"
    )
    print(f"report written: {out}")
    return out


# ──────────────────────────────────────────────────────────────────────────────
def _series(rows, scen, mode, phase, engine=None):
    """round -> duration for a given slice (ok only), sorted by round."""
    pts = {}
    for r in rows:
        if r["candidate"] != scen or r["compaction"] != mode or r["phase"] != phase:
            continue
        if r["status"] != "ok" or r["round"] < 1:
            continue
        if engine is not None and r["query_engine"] != engine:
            continue
        pts[r["round"]] = r["duration_s"]
    xs = sorted(pts)
    return xs, [pts[x] for x in xs]


def _grid(scenarios):
    n = len(scenarios)
    cols = 2 if n > 1 else 1
    rows_ = (n + cols - 1) // cols
    fig, axes = plt.subplots(rows_, cols, figsize=(6.4 * cols, 3.6 * rows_), squeeze=False)
    return fig, axes, cols


def _graphs(rows, results_dir, scenarios, modes, read_engines):
    imgs = []

    # G1: load time vs round, per scenario, line per compaction mode
    fig, axes, cols = _grid(scenarios)
    for i, s in enumerate(scenarios):
        ax = axes[i // cols][i % cols]
        for m in modes:
            xs, ys = _series(rows, s, m, "load")
            if xs:
                ax.plot(xs, ys, marker="o", label=m)
        ax.set_title(s); ax.set_xlabel("round"); ax.set_ylabel("load (s)"); ax.legend(fontsize=7)
    fig.tight_layout(); p = results_dir / "fig_load.png"; fig.savefig(p, dpi=110); plt.close(fig)
    imgs.append(("적재 시간 vs 라운드 (compaction 모드별)", p.name))

    # G2: query p50 vs round, per scenario, line per engine×mode
    fig, axes, cols = _grid(scenarios)
    for i, s in enumerate(scenarios):
        ax = axes[i // cols][i % cols]
        any_line = False
        for e in read_engines:
            for m in modes:
                xs, ys = _series(rows, s, m, "query", engine=e)
                if xs:
                    ax.plot(xs, ys, marker=".", label=f"{e}/{m}"); any_line = True
        ax.set_title(s); ax.set_xlabel("round"); ax.set_ylabel("query p50 (s)")
        if any_line:
            ax.legend(fontsize=6)
    fig.tight_layout(); p = results_dir / "fig_query.png"; fig.savefig(p, dpi=110); plt.close(fig)
    imgs.append(("조회 지연 vs 라운드 (엔진/compaction별)", p.name))

    # G3: compaction time vs round
    has_comp = any(r["phase"] == "compact" for r in rows)
    if has_comp:
        fig, axes, cols = _grid(scenarios)
        for i, s in enumerate(scenarios):
            ax = axes[i // cols][i % cols]
            for m in modes:
                xs, ys = _series(rows, s, m, "compact")
                if xs:
                    ax.plot(xs, ys, marker="s", label=m)
            ax.set_title(s); ax.set_xlabel("round"); ax.set_ylabel("compaction (s)")
            ax.legend(fontsize=7)
        fig.tight_layout(); p = results_dir / "fig_compaction.png"; fig.savefig(p, dpi=110); plt.close(fig)
        imgs.append(("compaction 시간 vs 라운드", p.name))

    return imgs
