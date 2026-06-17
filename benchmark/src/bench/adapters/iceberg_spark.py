"""Iceberg (v2/v3) COW/MOR written by Spark against the Polaris catalog.

Spark is the writer (full v2/v3 + MERGE upsert) and one of the read engines; StarRocks is
the other read engine (reads the same Polaris-managed table, metadata cache disabled).
Compaction (Spark rewrite_data_files) is applied by the runner per the configured cadence;
for MOR it merges deletion vectors / position deletes back into data files — which is the
only way StarRocks can read an Iceberg v3 MOR table.
"""
from __future__ import annotations

import time

from .. import schema as S
from .base import Adapter, register

CATALOG = "ice"  # name in BOTH Spark (spark-defaults) and the StarRocks read catalog

# Polaris occasionally returns transient 403/503 under load; retry these.
_TRANSIENT = ("forbidden", "serviceunavailable", "service unavailable",
              "503", "unable to fetch", "connection reset", "timed out")


def _is_transient(e: Exception) -> bool:
    m = str(e).lower()
    return any(t in m for t in _TRANSIENT)


def _retry(fn, attempts: int = 5, delay: float = 4.0):
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            if i < attempts - 1 and _is_transient(e):
                time.sleep(delay)
                continue
            raise


@register("iceberg_spark")
class IcebergSparkAdapter(Adapter):
    def _fqtn_spark(self) -> str:
        return f"{CATALOG}.`{self.ctx.db}`.`{self.table}`"

    def _fqtn_sr(self) -> str:
        return f"`{CATALOG}`.`{self.ctx.db}`.`{self.table}`"

    def prepare(self) -> None:
        # StarRocks read-side catalog only needed if StarRocks is a read engine.
        if "starrocks" in self.cfg.read_engines:
            self.ctx.sr.ensure_iceberg_catalog(CATALOG)
        sp = self.ctx.spark
        # global defaults (e.g. zstd) merged under candidate props (candidate can override)
        props = {**self.cfg.iceberg_table_defaults, **self.candidate.table_properties}

        def _create():
            sp.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.`{self.ctx.db}`")
            sp.sql(f"DROP TABLE IF EXISTS {self._fqtn_spark()}")
            sp.sql(S.spark_iceberg_ddl(
                self._fqtn_spark(), self.cols, self.cfg.schema.char_len,
                self.candidate.iceberg_format_version, props))
        _retry(_create)

    def load_round(self, round_idx: int, s3_uri: str) -> int:
        sp = self.ctx.spark
        local = f"file:///staging/round_{round_idx:02d}.parquet"
        cols = ", ".join(f"`{c.name}`" for c in self.cols)
        fqtn = self._fqtn_spark()

        def _load():
            sp.sql(f"CREATE OR REPLACE TEMPORARY VIEW src AS SELECT * FROM parquet.`{local}`")
            if round_idx == 0 or not self.supports_upsert():
                sp.sql(f"INSERT INTO {fqtn} ({cols}) SELECT {cols} FROM src")
            else:
                sp.sql(
                    f"MERGE INTO {fqtn} t USING src s ON t.`{self.pk_col}` = s.`{self.pk_col}` "
                    f"WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
        _retry(_load)
        return int(sp.scalar(f"SELECT count(*) FROM {fqtn}") or 0)

    def compact(self) -> dict:
        """Rewrite data files, applying deletes/deletion-vectors (delete-file-threshold=1
        forces any file with deletes to be rewritten). Returns the procedure's summary row."""
        tbl = f"{self.ctx.db}.{self.table}"
        rows = self.ctx.spark.session().sql(
            f"CALL {CATALOG}.system.rewrite_data_files("
            f"table => '{tbl}', "
            f"options => map('delete-file-threshold','1','min-input-files','1'))"
        ).collect()
        if rows:
            r = rows[0].asDict()
            return {"rewritten_data_files": r.get("rewritten_data_files_count"),
                    "added_data_files": r.get("added_data_files_count")}
        return {}

    def maintain(self) -> dict:
        """Keep only the latest snapshot and delete orphan files. Run after each compaction;
        timed separately from the rewrite. Bounds storage (snapshots/orphans otherwise grow).

        CALL args must be literals (current_timestamp() doesn't parse), so we read Spark's
        current time and pass a TIMESTAMP literal slightly in the future so all just-written
        snapshots/files qualify."""
        from datetime import timedelta
        tbl = f"{self.ctx.db}.{self.table}"
        sp = self.ctx.spark.session()
        now = sp.sql("SELECT current_timestamp() AS t").collect()[0][0]
        ts = (now + timedelta(seconds=2)).strftime("%Y-%m-%d %H:%M:%S")
        out = {}
        try:
            exp = sp.sql(
                f"CALL {CATALOG}.system.expire_snapshots("
                f"table => '{tbl}', older_than => TIMESTAMP '{ts}', retain_last => 1)").collect()
            if exp:
                d = exp[0].asDict()
                out["deleted_data_files"] = d.get("deleted_data_files_count")
        except Exception as e:  # noqa: BLE001
            out["expire_err"] = str(e)[:140]
        try:
            orp = sp.sql(
                f"CALL {CATALOG}.system.remove_orphan_files("
                f"table => '{tbl}', older_than => TIMESTAMP '{ts}')").collect()
            out["orphans_removed"] = len(orp)
        except Exception as e:  # noqa: BLE001
            out["orphan_err"] = str(e)[:140]
        return out

    def before_query(self, engine: str) -> None:
        if engine == "starrocks":
            self.ctx.sr.refresh_iceberg(CATALOG, self.ctx.db, self.table)

    def run_query(self, round_ids: list[int], engine: str) -> int:
        ids = ", ".join(str(r) for r in round_ids)
        if engine == "starrocks":
            val = self.ctx.sr.scalar(
                f"SELECT count(*) FROM {self._fqtn_sr()} WHERE `{self.round_col}` IN ({ids})")
        else:  # spark
            val = self.ctx.spark.scalar(
                f"SELECT count(*) FROM {self._fqtn_spark()} WHERE `{self.round_col}` IN ({ids})")
        return int(val or 0)

    def cleanup(self) -> None:
        # PURGE deletes the data/metadata files from S3 too — without it every combo's
        # table files leak into MinIO and accumulate across the matrix (disk blow-up).
        try:
            self.ctx.spark.sql(f"DROP TABLE IF EXISTS {self._fqtn_spark()} PURGE")
        except Exception:  # noqa: BLE001
            try:
                self.ctx.spark.sql(f"DROP TABLE IF EXISTS {self._fqtn_spark()}")
            except Exception:  # noqa: BLE001
                pass
