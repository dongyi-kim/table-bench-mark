"""Deterministic, seeded pre-materialization of all round payloads.

Random generation is *never* part of a benchmark timing. This module produces the
exact same bytes for every candidate, writes them to local `staging/` (for hashing &
inspection) and uploads them to MinIO (`s3://<bucket>/staging/`) where every engine
reads them.

Round layout (upsert/mutation workload):
  - round_00.parquet : initial seed (all new PKs, round_id=0)
  - round_RR.parquet : (1-U) new PKs + U updates to existing PKs, round_id=RR
"""
from __future__ import annotations

import hashlib
import json
import string
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs as pafs

from .config import BenchConfig
from .schema import Column, arrow_schema, build_columns

_ALPHABET = np.array(list(string.ascii_uppercase + string.digits))


def _s3fs(cfg: BenchConfig) -> pafs.S3FileSystem:
    # endpoint_override wants host:port without scheme.
    host = cfg.s3.endpoint.split("://", 1)[-1]
    scheme = "http" if cfg.s3.endpoint.startswith("http://") else "https"
    return pafs.S3FileSystem(
        access_key=cfg.s3.access_key,
        secret_key=cfg.s3.secret_key,
        endpoint_override=host,
        scheme=scheme,
        region=cfg.s3.region,
    )


def _gen_value_columns(cols: list[Column], n: int, rng: np.random.Generator,
                       char_len: int) -> dict[str, np.ndarray]:
    """Generate the non-key value columns (everything except pk_id/round_id)."""
    data: dict[str, np.ndarray] = {}
    for c in cols:
        if c.is_pk or c.is_round:
            continue
        if c.kind == "double":
            data[c.name] = rng.standard_normal(n).astype(np.float64)
        elif c.kind == "int":
            data[c.name] = rng.integers(-1_000_000, 1_000_000, size=n, dtype=np.int32)
        elif c.kind == "char":
            idx = rng.integers(0, len(_ALPHABET), size=(n, char_len))
            picked = _ALPHABET[idx]  # (n, char_len) of single chars
            data[c.name] = np.array(["".join(row) for row in picked], dtype=object)
    return data


def _build_table(cols: list[Column], pk: np.ndarray, round_id: int,
                 rng: np.random.Generator, char_len: int, schema: pa.Schema) -> pa.Table:
    n = len(pk)
    arrays: dict[str, np.ndarray] = {
        cols[0].name: pk.astype(np.int64),
        cols[1].name: np.full(n, round_id, dtype=np.int32),
    }
    arrays.update(_gen_value_columns(cols, n, rng, char_len))
    columns = [pa.array(arrays[c.name]) for c in cols]
    return pa.table(columns, schema=schema)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def generate(cfg: BenchConfig, local_dir: Path = Path("/staging"),
             upload: bool = True) -> dict:
    """Materialize all rounds. Returns a manifest dict (also written to staging/manifest.json)."""
    cols = build_columns(cfg.schema)
    schema = arrow_schema(cols, cfg.schema.char_len)
    wl = cfg.workload
    char_len = cfg.schema.char_len

    local_dir.mkdir(parents=True, exist_ok=True)
    s3 = _s3fs(cfg) if upload else None

    # last-write round per pk, for computing expected "recent" query counts.
    max_pks = wl.initial_rows + wl.rounds * wl.rows_per_round + 1
    last_round = np.full(max_pks, -1, dtype=np.int64)

    rounds_meta = []
    next_pk = 0
    existing_pks = np.empty(0, dtype=np.int64)

    def emit(round_id: int, pk: np.ndarray, rng: np.random.Generator) -> dict:
        table = _build_table(cols, pk, round_id, rng, char_len, schema)
        fname = f"round_{round_id:02d}.parquet"
        lpath = local_dir / fname
        pq.write_table(table, lpath, compression="zstd")
        meta = {
            "round": round_id,
            "file": fname,
            "rows": len(pk),
            "sha256": _sha256(lpath),
            "pk_min": int(pk.min()),
            "pk_max": int(pk.max()),
        }
        if s3 is not None:
            dest = f"{cfg.warehouse.bucket}/{cfg.warehouse.staging_prefix}/{fname}"
            with s3.open_output_stream(dest) as out:
                pq.write_table(table, out, compression="zstd")
            meta["s3"] = cfg.staging_s3_uri(fname)
        return meta

    # ---- round 0: initial seed -------------------------------------------------
    rng0 = np.random.default_rng(cfg.seed)
    pk0 = np.arange(0, wl.initial_rows, dtype=np.int64)
    last_round[pk0] = 0
    next_pk = wl.initial_rows
    existing_pks = pk0.copy()
    m = emit(0, pk0, rng0)
    m["new"] = int(wl.initial_rows)
    m["updates"] = 0
    m["expected_recent"] = None  # n/a before round 1
    rounds_meta.append(m)

    # ---- rounds 1..R: upsert mix ----------------------------------------------
    update_count = int(round(wl.rows_per_round * wl.update_ratio))
    new_count = wl.rows_per_round - update_count

    for r in range(1, wl.rounds + 1):
        rng = np.random.default_rng(cfg.seed + r)
        new_pks = np.arange(next_pk, next_pk + new_count, dtype=np.int64)
        next_pk += new_count
        # sample updates from currently-existing pks (without replacement).
        upd = rng.choice(existing_pks, size=min(update_count, len(existing_pks)),
                         replace=False) if update_count else np.empty(0, dtype=np.int64)
        pk = np.concatenate([new_pks, upd])
        rng.shuffle(pk)

        last_round[new_pks] = r
        last_round[upd] = r
        existing_pks = np.concatenate([existing_pks, new_pks])

        # expected rows for query round_id IN {r-1, r} as of right after round r.
        assigned = last_round[last_round >= 0]
        expected_recent = int(np.count_nonzero(assigned == r) + np.count_nonzero(assigned == (r - 1)))

        m = emit(r, pk, rng)
        m["new"] = int(len(new_pks))
        m["updates"] = int(len(upd))
        m["expected_recent"] = expected_recent
        rounds_meta.append(m)

    manifest = {
        "seed": cfg.seed,
        "schema": {
            "double_cols": cfg.schema.double_cols,
            "int_cols": cfg.schema.int_cols,
            "char_cols": cfg.schema.char_cols,
            "char_len": cfg.schema.char_len,
            "total_columns": len(cols),
        },
        "workload": {
            "initial_rows": wl.initial_rows,
            "rows_per_round": wl.rows_per_round,
            "rounds": wl.rounds,
            "update_ratio": wl.update_ratio,
            "query_recent_rounds": wl.query_recent_rounds,
        },
        "rounds": rounds_meta,
        "columns": [{"name": c.name, "kind": c.kind} for c in cols],
    }
    (local_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def load_manifest(local_dir: Path = Path("/staging")) -> dict:
    return json.loads((local_dir / "manifest.json").read_text())
