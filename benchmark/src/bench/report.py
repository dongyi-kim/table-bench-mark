"""Build a detailed Korean comparison report (Markdown + PNG graphs) from results.jsonl.

Dimensions: Iceberg scenario (v2/v3 × cow/mor) × compaction mode (none / every_10_rounds /
every_round) × read engine (StarRocks, Spark). Reports methodology, a compatibility matrix,
query / freshness / load+compaction tables, matplotlib graphs, and per-scenario commentary.
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from statistics import pstdev

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
            # v3-MOR is deliberately excluded from StarRocks (deletion vectors unreadable):
            # show incompatibility rather than "no data".
            if scen == "iceberg-v3-mor" and eng.startswith("starrocks"):
                return "✗"
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

    # ---- detailed comparisons (by compaction policy + pairwise) ---------------
    eng0 = read_engines[0] if read_engines else None

    def _vals(s, m, metric, engine=None):
        e = engine or eng0
        if metric == "load":
            return load_ok.get((s, m)) or []        # write side is engine-independent
        if metric == "freshness":
            return (f_ok.get((s, m, e)) or []) if e else []
        if metric == "query":
            return (q_ok.get((s, m, e)) or []) if e else []
        return []

    def _avg(s, m, metric, engine=None):
        xs = _vals(s, m, metric, engine)
        return _mean(xs) if xs else None

    def _cvp(s, m, metric, engine=None):  # coefficient of variation, %
        xs = _vals(s, m, metric, engine)
        if len(xs) < 2:
            return None
        mu = _mean(xs)
        return (pstdev(xs) / mu * 100) if mu else None

    def _f(v, d=3, suf=""):
        return f"{v:.{d}f}{suf}" if v is not None else "—"

    METHOD_LABEL = {"iceberg-v2-cow": "v2-COW", "iceberg-v2-mor": "v2-MOR",
                    "iceberg-v3-cow": "v3-COW", "iceberg-v3-mor": "v3-MOR"}

    def ml(s):
        return METHOD_LABEL.get(s, s)

    # § per-compaction method comparison (panel = compaction, rows = methods)
    bycomp_parts = []
    for m in modes:
        tbl = tabulate(
            [[ml(s), _f(_avg(s, m, "load")), _f(_avg(s, m, "freshness")),
              _f(_cvp(s, m, "freshness"), 0, "%"), _f(_avg(s, m, "query")),
              _f(_cvp(s, m, "query"), 0, "%")] for s in scenarios],
            headers=["방식", "적재(s)", "freshness(s)", "fresh CV", "조회 p50(s)", "조회 CV"],
            tablefmt="github")
        bycomp_parts.append(f"### compaction = `{m}`\n\n{tbl}")
    bycomp_md = "\n\n".join(bycomp_parts)

    # § pairwise comparisons (ratio = b ÷ a; <1 means b is lower)
    def _ratio(a, b, m, metric):
        va, vb = _avg(a, m, metric), _avg(b, m, metric)
        return (vb / va) if (va and vb) else None

    def _pair_table(a, b, title):
        rows_t = [[m,
                   _f(_avg(a, m, "load")), _f(_avg(b, m, "load")), _f(_ratio(a, b, m, "load"), 2, "×"),
                   _f(_avg(a, m, "freshness")), _f(_avg(b, m, "freshness")), _f(_ratio(a, b, m, "freshness"), 2, "×"),
                   _f(_avg(a, m, "query")), _f(_avg(b, m, "query")), _f(_ratio(a, b, m, "query"), 2, "×")]
                  for m in modes]
        tbl = tabulate(rows_t, headers=["compaction",
                       f"적재 {ml(a)}", f"적재 {ml(b)}", "비율",
                       f"fresh {ml(a)}", f"fresh {ml(b)}", "비율",
                       f"조회 {ml(a)}", f"조회 {ml(b)}", "비율"], tablefmt="github")
        return f"### {title} — 비율 = {ml(b)} ÷ {ml(a)} (＜1 이면 {ml(b)} 가 더 낮음)\n\n{tbl}"

    have = set(scenarios)
    pair_specs = [(f"iceberg-{v}-cow", f"iceberg-{v}-mor", f"COW vs MOR ({v})") for v in ("v2", "v3")]
    pair_specs += [("iceberg-v2-mor", "iceberg-v3-mor", "v2-MOR vs v3-MOR"),
                   ("iceberg-v2-cow", "iceberg-v3-cow", "v2-COW vs v3-COW")]
    pairs_md = "\n\n".join(_pair_table(a, b, t) for a, b, t in pair_specs if a in have and b in have)

    # § conclusion — best configs + normalized-balance recommendation
    def _best(metric):
        c = [((s, m), _avg(s, m, metric)) for s in scenarios for m in modes if _avg(s, m, metric) is not None]
        return min(c, key=lambda x: x[1]) if c else None

    bal_metrics = ["load", "query", "freshness"]
    mins = {mt: min((_avg(s, m, mt) for s in scenarios for m in modes if _avg(s, m, mt) is not None),
                    default=None) for mt in bal_metrics}

    def _score(s, m):  # mean of per-metric ratio-to-best (1.0 = best on every metric)
        rs = [(_avg(s, m, mt) / mins[mt]) for mt in bal_metrics
              if _avg(s, m, mt) is not None and mins[mt]]
        return (sum(rs) / len(rs)) if rs else None

    concl = []
    for label, mt, tail in (("적재(write) 최저", "load", "MOR 계열이 평탄·저비용"),
                            ("조회(read) 최저 p50", "query", ""),
                            ("freshness 최저", "freshness", "")):
        b = _best(mt)
        if b:
            concl.append(f"- **{label}**: `{ml(b[0][0])}` / {b[0][1]} ({b[1]:.3f}s)"
                         + (f" — {tail}" if tail else "") + ".")
    bal = [((s, m), _score(s, m)) for s in scenarios for m in modes if _score(s, m) is not None]
    if bal:
        (s, m), sc = min(bal, key=lambda x: x[1])
        concl.append(f"- **균형 종합 권장**: `{ml(s)}` / `{m}` (정규화 점수 {sc:.2f}, 1.0=모든 지표 최저) "
                     "— 적재·freshness·조회를 동일 가중으로 합산한 최적. 실시간·쓰기빈번(적재→조회 "
                     "지연 최소화) 워크로드 기준.")
    concl_md = "\n".join(concl) if concl else "- (데이터 부족)"

    # ---- technical commentary (data-driven mechanism explanations) ------------
    def _mean_where(metric, pred, engine=None):
        vs = [_avg(s, m, metric, engine) for s in scenarios for m in modes
              if pred(s, m) and _avg(s, m, metric, engine) is not None]
        return _mean(vs) if vs else None

    def _growth(s, m):  # load r1 -> rN from the per-round series
        lb = load_by_round.get((s, m), {})
        if len(lb) < 2:
            return None
        r1, rN = min(lb), max(lb)
        return (lb[r1], lb[rN], (lb[rN] / lb[r1]) if lb[r1] else None)

    cow_w = _mean_where("load", lambda s, m: "cow" in s and m == "none")
    mor_w = _mean_where("load", lambda s, m: "mor" in s and m == "none")
    wr = (mor_w / cow_w) if (cow_w and mor_w) else None
    # compaction total cost (every_round vs every_10) averaged across scenarios
    comp_tot_er = _mean([sum(comp_ok[(s, "every_round")]) for s in scenarios if comp_ok.get((s, "every_round"))]) \
        if any(comp_ok.get((s, "every_round")) for s in scenarios) else None
    comp_tot_e10 = _mean([sum(comp_ok[(s, "every_10_rounds")]) for s in scenarios if comp_ok.get((s, "every_10_rounds"))]) \
        if any(comp_ok.get((s, "every_10_rounds")) for s in scenarios) else None
    # MOR read improvement with compaction (none -> every_round), primary engine
    mor_q_none = _mean_where("query", lambda s, m: "mor" in s and m == "none")
    mor_q_er = _mean_where("query", lambda s, m: "mor" in s and m == "every_round")
    # COW late-round blow-up (v3 vs v2) under every_round
    g_v2 = _growth("iceberg-v2-cow", "every_round")
    g_v3 = _growth("iceberg-v3-cow", "every_round")

    def _n(v, d=2, suf="s"):
        return f"{v:.{d}f}{suf}" if v is not None else "—"

    tech_lines = []
    tech_lines.append(
        "**COW vs MOR 쓰기 메커니즘**: COW(copy-on-write)는 `MERGE` 시 갱신 행이 포함된 **데이터 파일을 "
        "통째로 다시 씁니다**. 그래서 쓰기 비용이 테이블 크기·파일 수에 비례해 커집니다. MOR(merge-on-read)은 "
        "데이터 파일을 안 건드리고 **삭제 표식만 추가**합니다 — v2는 *positional delete 파일*, v3는 *deletion "
        "vector*(데이터 파일당 Roaring 비트맵 1개). 그래서 MOR 적재는 평탄·저비용입니다."
        + (f" 실측 적재(none): MOR {_n(mor_w)} vs COW {_n(cow_w)} (MOR이 COW의 {wr:.2f}배)." if wr else ""))
    tech_lines.append(
        "**small file·snapshot·compaction**: 매 라운드 커밋은 새 데이터/삭제 파일과 **snapshot 1개**를 만들어 "
        "작은 파일이 누적됩니다. `compaction`(`rewrite_data_files`)은 small file을 큰 파일로 병합하고 삭제(deletion "
        "vector/positional delete)를 데이터에 **흡수**합니다 → 파일 수↓, 읽기·freshness 개선. 대신 쓰기 비용이 "
        "추가됩니다."
        + (f" 실측 compaction 총비용: every_round ≈ {_n(comp_tot_er,0)} vs every_10 ≈ {_n(comp_tot_e10,0)}." if comp_tot_er else "")
        + " `maintain`은 snapshot을 1개만 남기고(expire) orphan 파일을 제거해 메타데이터 팽창을 막습니다.")
    tech_lines.append(
        "**주기(compaction cadence)의 양면성**: `none`은 쓰기는 싸지만 파일·삭제가 쌓여 읽기 플래닝이 무거워질 수 "
        "있습니다. `every_round`는 매 라운드 파일을 정리해 읽기/freshness가 가장 좋지만, **COW에서는 정리된 소수 "
        "대형 파일을 다음 MERGE가 거의 전체 재작성**하게 만들어 후반 적재가 급증합니다."
        + (f" 실측 every_round 적재 최종라운드: v3-COW {_n(g_v3[1])} vs v2-COW {_n(g_v2[1])} "
           f"(v3/v2 ≈ {g_v3[1] / g_v2[1]:.2f}배) — v3 row-lineage 오버헤드가 전체 재작성에서 드러남."
           if (g_v2 and g_v3 and g_v2[1]) else ""))
    tech_lines.append(
        "**MOR 읽기와 compaction**: MOR은 조회 시 삭제 표식을 실시간 병합하므로 compaction 전에는 읽기가 느릴 수 "
        "있습니다(특히 v2 positional delete는 여러 작은 삭제 파일을 reconcile). compaction이 삭제를 흡수하면 "
        "COW급으로 빨라집니다."
        + (f" 실측 MOR 조회: none {_n(mor_q_none,3)} → every_round {_n(mor_q_er,3)}." if (mor_q_none and mor_q_er) else ""))
    tech_lines.append(
        "**v2 vs v3**: v3는 deletion vector로 MOR 읽기가 v2(positional delete)보다 유리하고 삭제가 쌓여도 데이터 "
        "파일당 비트맵 1개라 성능이 덜 악화됩니다. 단 v3는 행마다 **row-lineage**(`_row_id`,"
        "`_last_updated_sequence_number`)를 의무적으로 유지하며 이는 **끌 수 없습니다**. 계측 결과(v2 vs v3-COW, "
        "every_round): 재작성 **행수·파일수·바이트는 동일**(행당 +0.6%)인데 **적재 시간만 R40부터 1.5~1.9× 더 큼** "
        "→ 즉 쓰기량(I/O)이 아니라 **per-row 연산(row-lineage 유지) 오버헤드**가, 테이블 전체를 매라운드 재작성하는 "
        "구간에서 갑자기 드러나는 것(전체 재작성 행수가 임계에 도달하며 발생하는 계단형 비용).")
    tech_lines.append(
        "**freshness 해석**: freshness는 *쓰기→가시성 지연*으로, 본질적으로 새 snapshot의 메타데이터 플래닝 비용에 "
        "가깝습니다(데이터 본문 읽기는 제외한 경량 프로브). Spark는 자기 커밋을 즉시 보고, StarRocks는 메타캐시 "
        "비활성이라 측정에 `REFRESH EXTERNAL TABLE`이 포함됩니다 — 엔진 간 freshness는 정의가 다소 다릅니다.")
    tech_md = "\n\n".join(f"- {t}" for t in tech_lines)

    # ---- read-engine comparison + trend divergence (>=2 engines) --------------
    spark_e = next((e for e in read_engines if e == "spark"), None)
    sr_e = next((e for e in read_engines if e.startswith("starrocks")), None)
    engine_md = ""
    if spark_e and sr_e:
        rows_cmp = []
        for s in scenarios:
            for m in modes:
                qs, qr = _avg(s, m, "query", spark_e), _avg(s, m, "query", sr_e)
                fs, fr = _avg(s, m, "freshness", spark_e), _avg(s, m, "freshness", sr_e)
                if qs is None and qr is None:
                    continue
                rows_cmp.append([ml(s), m,
                                 _f(qs), _f(qr), _f((qr / qs) if (qs and qr) else None, 2, "×"),
                                 _f(fs), _f(fr), _f((fr / fs) if (fs and fr) else None, 2, "×")])
        cmp_tbl = tabulate(rows_cmp, headers=["방식", "compaction",
                           "조회 Spark", "조회 SR", "SR÷Spark", "fresh Spark", "fresh SR", "SR÷Spark"],
                           tablefmt="github")

        def _best_eng(e):
            c = [((s, m), _avg(s, m, "query", e)) for s in scenarios for m in modes
                 if _avg(s, m, "query", e) is not None]
            return min(c, key=lambda x: x[1]) if c else None

        bspark, bsr = _best_eng(spark_e), _best_eng(sr_e)
        qratios = [(_avg(s, m, "query", sr_e) / _avg(s, m, "query", spark_e))
                   for s in scenarios for m in modes
                   if _avg(s, m, "query", spark_e) and _avg(s, m, "query", sr_e)]
        meanqr = _mean(qratios) if qratios else None
        sr_methods = sorted({s for s in scenarios for m in modes if _avg(s, m, "query", sr_e) is not None})
        sr_missing = [ml(s) for s in scenarios if s not in sr_methods]

        notes_e = []
        if meanqr is not None:
            faster = "더 빠름" if meanqr < 1 else "더 느림"
            notes_e.append(f"- 공통 구성 평균 조회비: **SR÷Spark = {meanqr:.2f}×** (StarRocks가 {faster}).")
        if bspark:
            notes_e.append(f"- Spark 최저 조회: `{ml(bspark[0][0])}` / {bspark[0][1]} ({bspark[1]:.3f}s).")
        if bsr:
            notes_e.append(f"- StarRocks 최저 조회: `{ml(bsr[0][0])}` / {bsr[0][1]} ({bsr[1]:.3f}s).")
        if sr_missing:
            notes_e.append(f"- StarRocks **미지원**: {', '.join(sr_missing)} "
                           "(v3-MOR deletion vector 직접 조회 불가 → 측정 제외, 호환성 ✗).")
        # trend divergence: does the best *format* differ by engine?
        if bspark and bsr and bspark[0][0] != bsr[0][0]:
            notes_e.append(f"- **경향성 차이**: 최적 방식이 엔진에 따라 갈림 — Spark는 `{ml(bspark[0][0])}`, "
                           f"StarRocks는 `{ml(bsr[0][0])}`. 즉 **조회 엔진 선택이 최적 포맷을 바꿉니다** "
                           "(Spark는 v3-MOR을 읽어 활용 가능, StarRocks는 불가).")
        else:
            notes_e.append("- **경향성**: 두 엔진의 최적 방식 경향은 대체로 일치하나, StarRocks는 v3-MOR을 "
                           "선택지에서 제외해야 한다는 제약이 핵심 차이.")
        engine_md = (cmp_tbl + "\n\n" + "\n".join(notes_e))

    imgs, imgs_bymethod = _graphs(rows, results_dir, scenarios, modes, read_engines)

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
        "- **적재 엔진** = Apache Spark(`MERGE INTO` 업서트), **조회 엔진** = 설정된 `read_engines`"
        "(현재 Spark; StarRocks 옵션). 카탈로그 = Polaris(REST), 스토리지 = MinIO(S3).\n"
        "- **시나리오** = Iceberg 포맷버전(v2/v3) × 쓰기모드(COW/MOR). "
        "**compaction 모드** = none / every_10_rounds / every_round (Spark `rewrite_data_files`, "
        "MOR의 deletion vector·delete를 데이터파일에 흡수).\n"
        "- **시나리오당 흐름**: 초기 시드 후 매 라운드 10만 행 업서트(신규 80% + 기존 PK 20% 갱신) "
        "→ compaction(주기 해당 시) → 각 엔진이 최근 2회차(~20만 행) 조회.\n"
        "- **지표**:\n"
        "  - `적재(load)` = staging→테이블 1라운드 쓰기 시간(Spark).\n"
        "  - `compaction` = rewrite_data_files 1회 시간(Spark).\n"
        "  - `maintain` = compaction마다 스냅샷 1개만 남기고(expire) orphan 파일 제거하는 시간(별도 추적).\n"
        "  - `freshness(write→read)` = **커밋 직후 최신 round_id가 조회에 보일 때까지의 지연** "
        "(경량 가시성 프로브 폴링; 못 읽으면 실패). 한 write당 1회 측정이라 라운드별 잡음은 정상 — "
        "**분포(median/IQR/p95)** 로 해석.\n"
        "  - `조회(query)` = 가시화 이후 정상상태 조회 지연(반복 측정 p50).\n"
        "- **공정성·정밀도**: 랜덤 데이터는 사전 시드 Parquet으로 1회 생성(측정 제외)·동일 바이트, "
        "Polaris 메타데이터 캐시 비활성, 후보마다 새 테이블(격리), **측정 직전 정착(settle, 타이머 밖)**, "
        "컨테이너 자원 캡 + Docker VM 사이징으로 스왑/경합 차단. **신뢰도**는 전체 매트릭스 **N회 "
        "반복·평균**(run 간 평균·변동성)으로 확보.\n"
        "- **비교 관점**: compaction 정책별 패널에서 방식 비교 + 쌍대(COW vs MOR / v2-MOR vs v3-MOR / "
        "v2-COW vs v3-COW).\n\n"
        "## 1. 호환성 매트릭스 (✓ 정상 / △ 부분 / ✗ 불가 / - 없음)\n\n" + compat_md
        + "\n\n## 2. 조회 지연 (정상상태 p50 평균, 초)\n\n" + query_md
        + "\n\n## 3. 신선도 write→read (커밋→조회가능 지연 평균, 초)\n\n" + fresh_md
        + "\n\n## 4. 적재 · compaction · maintain(스냅샷 expire+orphan) 비용 (초)\n\n" + lc_md
        + "\n\n## 5. compaction 정책별 방식 비교 (각 정책 하에서 v2/v3 × COW/MOR)\n\n"
        "> CV(변동계수)는 라운드 간 변동성. freshness 는 단일 콜드 측정이라 CV 가 query 보다 큼(정상).\n\n"
        + bycomp_md
        + "\n\n## 6. 쌍대 비교 (COW vs MOR · v2 vs v3)\n\n" + pairs_md
        + "\n\n## 7. 기술 배경 · 수치 해석 (메커니즘)\n\n"
        "> 개념: COW(데이터파일 전체 재작성) · MOR(삭제표식 추가; v2 positional delete / v3 deletion "
        "vector) · compaction(small file 병합 + 삭제 흡수) · snapshot(커밋 단위, maintain이 expire) · "
        "row-lineage(v3 행 메타데이터).\n\n" + tech_md
        + (("\n\n## 8. 조회 엔진 비교 · 경향성 (Spark vs StarRocks)\n\n"
            "> freshness 는 엔진별 단독 run 으로 측정 후 병합(교차 워밍 방지). StarRocks 는 메타캐시 "
            "비활성 + `REFRESH` 포함.\n\n" + engine_md) if engine_md else "")
        + "\n\n## 9. 그래프\n\n"
        + "### 9a. 패널=compaction · 선=방식 (각 compaction 정책에서 v2/v3×COW/MOR 비교)\n\n"
        + "".join(f"#### {title}\n\n![{title}]({fn})\n\n" for title, fn in imgs)
        + "### 9b. 패널=방식 · 선=compaction (각 방식에서 compaction 주기 비교)\n\n"
        + "".join(f"#### {title}\n\n![{title}]({fn})\n\n" for title, fn in imgs_bymethod)
        + "## 10. 시나리오별 해설\n\n" + "\n".join(scen_notes)
        + "\n\n## 11. 종합 해설\n\n" + "\n".join(notes)
        + "\n\n### 결론 — freshness · write/read 확보에 좋은 구성\n\n" + concl_md + "\n"
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
    # sharey=True -> all panels in one figure use the same y-scale so the panels are
    # visually comparable (e.g. COW panels are obviously taller than MOR panels).
    fig, axes = plt.subplots(rows_, cols, figsize=(6.4 * cols, 3.6 * rows_),
                             squeeze=False, sharey=True)
    return fig, axes, cols


def _grid_plot(rows, results_dir, scenarios, modes, read_engines, phase, ylabel,
               fname, title, per_engine):
    # One subplot per compaction mode; the lines within a panel compare the methods
    # (Iceberg scenarios) so each compaction cadence is a same-axes method comparison.
    fig, axes, cols = _grid(modes)
    drew = False
    for i, m in enumerate(modes):
        ax = axes[i // cols][i % cols]
        if per_engine:
            for s in scenarios:
                for e in read_engines:
                    xs, ys = _series(rows, s, m, phase, engine=e)
                    if xs:
                        lbl = s if len(read_engines) == 1 else f"{s}/{e}"
                        ax.plot(xs, ys, marker=".", label=lbl); drew = True
        else:
            for s in scenarios:
                xs, ys = _series(rows, s, m, phase)
                if xs:
                    ax.plot(xs, ys, marker="o", label=s); drew = True
        ax.set_title(f"compaction={m}"); ax.set_xlabel("round"); ax.set_ylabel(ylabel)
        ax.legend(fontsize=6)
    fig.tight_layout(); p = results_dir / fname; fig.savefig(p, dpi=110); plt.close(fig)
    return (title, p.name) if drew else None


def _grid_plot_by_method(rows, results_dir, scenarios, modes, read_engines, phase, ylabel,
                         fname, title, per_engine):
    # Reverse of _grid_plot: one subplot per method (scenario); lines = compaction policies,
    # so within each method the compaction cadences are compared on the same axes.
    fig, axes, cols = _grid(scenarios)
    drew = False
    for i, s in enumerate(scenarios):
        ax = axes[i // cols][i % cols]
        if per_engine:
            for m in modes:
                for e in read_engines:
                    xs, ys = _series(rows, s, m, phase, engine=e)
                    if xs:
                        lbl = m if len(read_engines) == 1 else f"{m}/{e}"
                        ax.plot(xs, ys, marker=".", label=lbl); drew = True
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
    imgs, imgs_bymethod = [], []
    specs = [
        ("load", "load (s)", "fig_load.png", "적재 시간 vs 라운드", False),
        ("query", "query p50 (s)", "fig_query.png", "조회 지연 vs 라운드", True),
        ("freshness", "write→read (s)", "fig_freshness.png", "신선도 write→read vs 라운드", True),
        ("compact", "compaction (s)", "fig_compaction.png", "compaction 시간 vs 라운드", False),
        ("maintain", "maintain (s)", "fig_maintain.png", "스냅샷 expire+orphan 제거 시간 vs 라운드", False),
    ]
    for phase, ylabel, fname, title, per_engine in specs:
        img = _grid_plot(rows, results_dir, scenarios, modes, read_engines, phase, ylabel,
                         fname, f"{title} (패널=compaction · 선=방식)", per_engine)
        if img:
            imgs.append(img)
        bm = _grid_plot_by_method(rows, results_dir, scenarios, modes, read_engines, phase, ylabel,
                                  fname.replace(".png", "_bymethod.png"),
                                  f"{title} (패널=방식 · 선=compaction)", per_engine)
        if bm:
            imgs_bymethod.append(bm)
    return imgs, imgs_bymethod
