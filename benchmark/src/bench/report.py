"""Build a detailed Korean comparison report (Markdown + PNG graphs) from results.jsonl.

Dimensions: Iceberg scenario (v2/v3 × cow/mor) × compaction mode (none / every_10_rounds /
every_round) × read engine (StarRocks, Spark). Reports methodology, a compatibility matrix,
query / freshness / load+compaction tables, matplotlib graphs, and per-scenario commentary.
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
MODE_ORDER = ["none", "every_10_rounds", "every_round"]


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
    rounds_total = max((r["round"] for r in rows), default=0)

    # ---- aggregates -----------------------------------------------------------
    q_ok = defaultdict(list)        # (scen,mode,engine) -> [p50...]
    q_status = defaultdict(set)
    f_ok = defaultdict(list)        # (scen,mode,engine) -> [freshness s]
    load_ok = defaultdict(list)     # (scen,mode) -> [load s] round>=1
    load_by_round = defaultdict(dict)  # (scen,mode) -> {round: load s}
    comp_ok = defaultdict(list)     # (scen,mode) -> [compact s]
    maint_ok = defaultdict(list)    # (scen,mode) -> [maintain s]
    last_err = {}

    for r in rows:
        scen, mode = r["candidate"], r["compaction"]
        if r["phase"] == "maintain" and r["status"] == "ok":
            maint_ok[(scen, mode)].append(r["duration_s"])
        if r["phase"] == "query":
            key = (scen, mode, r["query_engine"])
            q_status[key].add(r["status"])
            if r["status"] == "ok":
                q_ok[key].append(r["duration_s"])
            elif r.get("error"):
                last_err[key] = r["error"]
        elif r["phase"] == "freshness" and r["status"] == "ok":
            f_ok[(scen, mode, r["query_engine"])].append(r["duration_s"])
        elif r["phase"] == "load" and r["round"] >= 1 and r["status"] == "ok":
            load_ok[(scen, mode)].append(r["duration_s"])
            load_by_round[(scen, mode)][r["round"]] = r["duration_s"]
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

    headers = ["시나리오", "compaction"] + read_engines

    compat_md = tabulate(
        [[s, m] + [compat(s, m, e) for e in read_engines] for s in scenarios for m in modes],
        headers=headers, tablefmt="github")

    query_md = tabulate(
        [[s, m] + [f"{_mean(q_ok[(s, m, e)]):.3f}" if q_ok[(s, m, e)] else "—"
                   for e in read_engines] for s in scenarios for m in modes],
        headers=headers, tablefmt="github")

    fresh_md = tabulate(
        [[s, m] + [f"{_mean(f_ok[(s, m, e)]):.3f}" if f_ok[(s, m, e)] else "—"
                   for e in read_engines] for s in scenarios for m in modes],
        headers=headers, tablefmt="github")

    lc_md = tabulate(
        [[s, m,
          f"{_mean(load_ok[(s, m)]):.3f}" if load_ok[(s, m)] else "—",
          f"{_mean(comp_ok[(s, m)]):.3f}" if comp_ok[(s, m)] else "—",
          f"{sum(comp_ok[(s, m)]):.1f}" if comp_ok[(s, m)] else "0",
          f"{_mean(maint_ok[(s, m)]):.3f}" if maint_ok[(s, m)] else "—",
          f"{sum(maint_ok[(s, m)]):.1f}" if maint_ok[(s, m)] else "0"]
         for s in scenarios for m in modes],
        headers=["시나리오", "compaction", "적재 평균(s)", "compaction 평균(s)",
                 "compaction 총합(s)", "maintain 평균(s)", "maintain 총합(s)"],
        tablefmt="github")

    imgs = _graphs(rows, results_dir, scenarios, modes, read_engines)

    # ---- per-scenario commentary ---------------------------------------------
    sr = next((e for e in read_engines if e.startswith("starrocks")), None)
    sk = "spark" if "spark" in read_engines else None
    scen_notes = []
    for s in scenarios:
        bits = [f"**{s}**:"]
        lb = load_by_round.get((s, "none"), {})
        if lb:
            r1, rN = min(lb), max(lb)
            growth = lb[rN] / lb[r1] if lb[r1] else 0
            trend = "증가(테이블 성장 비례, COW 특성)" if growth >= 1.5 else "평탄(MOR 특성)"
            bits.append(f"적재 {lb[r1]:.1f}s→{lb[rN]:.1f}s ({trend}).")
        comp = comp_ok.get((s, "every_round"))
        if comp:
            bits.append(f"compaction 평균 {_mean(comp):.1f}s.")
        if sr:
            cs = ", ".join(f"{m}={compat(s, m, sr)}" for m in modes)
            bits.append(f"StarRocks 호환: {cs}.")
        if sr and q_ok.get((s, "none", sr)):
            bits.append(f"StarRocks 조회 p50 {_mean(q_ok[(s,'none',sr)]):.3f}s.")
        if sk and f_ok.get((s, "none", sk)):
            bits.append(f"Spark freshness {_mean(f_ok[(s,'none',sk)]):.3f}s.")
        scen_notes.append("- " + " ".join(bits))

    notes = []
    if sr and "iceberg-v3-mor" in scenarios:
        line = ", ".join(f"{m}={compat('iceberg-v3-mor', m, sr)}" for m in modes)
        notes.append(f"- **v3-MOR × {sr}** compaction별 호환성: {line} "
                     "→ deletion vector를 compaction으로 제거해야 StarRocks가 읽을 수 있음.")
    for e in read_engines:
        cands = [((s, m), _mean(q_ok[(s, m, e)])) for s in scenarios for m in modes if q_ok[(s, m, e)]]
        if cands:
            (s, m), v = min(cands, key=lambda x: x[1])
            notes.append(f"- **{e}** 최저 조회 지연: `{s}` / {m} ({v:.3f}s)")

    out = results_dir / "report.md"
    out.write_text(
        "# table-bench-mark 결과 리포트\n\n"
        f"결과 디렉터리: `{results_dir.name}` · 라운드 수: {rounds_total} · 압축: zstd(Parquet)\n\n"
        "## 0. 방법론 · 지표 정의\n\n"
        "- **목적**: 쓰기가 빈번한 워크로드에서 Iceberg 구성별 **적재→조회 지연**을 공정하게 비교.\n"
        "- **적재 엔진** = Apache Spark(`MERGE INTO` 업서트), **조회 엔진** = StarRocks·Spark "
        "(동일 테이블을 두 엔진이 각각 조회). 카탈로그 = Polaris(REST), 스토리지 = MinIO(S3).\n"
        "- **시나리오** = Iceberg 포맷버전(v2/v3) × 쓰기모드(COW/MOR). "
        "**compaction 모드** = none / every_10_rounds / every_round (Spark `rewrite_data_files`, "
        "MOR의 deletion vector·delete를 데이터파일에 흡수).\n"
        "- **시나리오당 흐름**: 초기 시드 후 매 라운드 10만 행 업서트(신규 80% + 기존 PK 20% 갱신) "
        "→ compaction(주기 해당 시) → 각 엔진이 최근 2회차(~20만 행) 조회.\n"
        "- **지표**:\n"
        "  - `적재(load)` = staging→테이블 1라운드 쓰기 시간(Spark).\n"
        "  - `compaction` = rewrite_data_files 1회 시간(Spark).\n"
        "  - `maintain` = compaction마다 스냅샷 1개만 남기고(expire) orphan 파일 제거하는 시간(별도 추적).\n"
        "  - `freshness(write→read)` = **커밋 직후 새 데이터가 조회 가능해질 때까지의 지연** "
        "(폴링; 엔진이 못 읽으면 실패로 기록).\n"
        "  - `조회(query)` = 가시화 이후 정상상태 조회 지연(반복 측정 p50).\n"
        "- **공정성**: 랜덤 데이터는 사전 시드 Parquet으로 1회 생성(측정 제외), 모든 시나리오 동일 "
        "바이트, Polaris 메타데이터 캐시 비활성, 후보마다 새 테이블(격리).\n\n"
        "## 1. 호환성 매트릭스 (✓ 정상 / △ 부분 / ✗ 불가 / - 없음)\n\n" + compat_md
        + "\n\n## 2. 조회 지연 (정상상태 p50 평균, 초)\n\n" + query_md
        + "\n\n## 3. 신선도 write→read (커밋→조회가능 지연 평균, 초)\n\n" + fresh_md
        + "\n\n## 4. 적재 · compaction · maintain(스냅샷 expire+orphan) 비용 (초)\n\n" + lc_md
        + "\n\n## 5. 그래프\n\n"
        + "".join(f"### {title}\n\n![{title}]({fn})\n\n" for title, fn in imgs)
        + "## 6. 시나리오별 해설\n\n" + "\n".join(scen_notes)
        + "\n\n## 7. 종합 해설\n\n" + "\n".join(notes) + "\n"
    )
    print(f"report written: {out}")
    return out


# ──────────────────────────────────────────────────────────────────────────────
def _series(rows, scen, mode, phase, engine=None):
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


def _grid_plot(rows, results_dir, scenarios, modes, read_engines, phase, ylabel,
               fname, title, per_engine):
    fig, axes, cols = _grid(scenarios)
    drew = False
    for i, s in enumerate(scenarios):
        ax = axes[i // cols][i % cols]
        if per_engine:
            for e in read_engines:
                for m in modes:
                    xs, ys = _series(rows, s, m, phase, engine=e)
                    if xs:
                        ax.plot(xs, ys, marker=".", label=f"{e}/{m}"); drew = True
        else:
            for m in modes:
                xs, ys = _series(rows, s, m, phase)
                if xs:
                    ax.plot(xs, ys, marker="o", label=m); drew = True
        ax.set_title(s); ax.set_xlabel("round"); ax.set_ylabel(ylabel)
        ax.legend(fontsize=6)
    fig.tight_layout(); p = results_dir / fname; fig.savefig(p, dpi=110); plt.close(fig)
    return (title, p.name) if drew else None


def _graphs(rows, results_dir, scenarios, modes, read_engines):
    imgs = []
    specs = [
        ("load", "load (s)", "fig_load.png", "적재 시간 vs 라운드 (compaction 모드별)", False),
        ("query", "query p50 (s)", "fig_query.png", "조회 지연 vs 라운드 (엔진/compaction별)", True),
        ("freshness", "write→read (s)", "fig_freshness.png", "신선도 write→read vs 라운드 (엔진/compaction별)", True),
        ("compact", "compaction (s)", "fig_compaction.png", "compaction 시간 vs 라운드", False),
        ("maintain", "maintain (s)", "fig_maintain.png", "스냅샷 expire+orphan 제거 시간 vs 라운드", False),
    ]
    for phase, ylabel, fname, title, per_engine in specs:
        img = _grid_plot(rows, results_dir, scenarios, modes, read_engines,
                         phase, ylabel, fname, title, per_engine)
        if img:
            imgs.append(img)
    return imgs
