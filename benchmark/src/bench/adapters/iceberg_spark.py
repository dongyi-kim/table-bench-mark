"""Iceberg (v2/v3) COW/MOR written by Spark against the Polaris catalog.

Spark is the writer (full v2/v3 + MERGE upsert) and one of the read engines; StarRocks is
the other read engine (reads the same Polaris-managed table, metadata cache disabled).
Compaction (Spark rewrite_data_files) is applied by the runner per the configured cadence;
for MOR it merges deletion vectors / position deletes back into data files — which is the
only way StarRocks can read an Iceberg v3 MOR table.
"""
from __future__ import annotations

from .. import schema as S
from .base import Adapter, register

CATALOG = "ice"  # name in BOTH Spark (spark-defaults) and the StarRocks read catalog


@register("iceberg_spark")
class IcebergSparkAdapter(Adapter):
    def _fqtn_spark(self) -> str:
        return f"{CATALOG}.`{self.ctx.db}`.`{self.table}`"

    def _fqtn_sr(self) -> str:
        return f"`{CATALOG}`.`{self.ctx.db}`.`{self.table}`"

    def prepare(self) -> None:
        self.ctx.sr.ensure_iceberg_catalog(CATALOG)   # StarRocks read-side catalog
        sp = self.ctx.spark
        sp.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.`{self.ctx.db}`")
        sp.sql(f"DROP TABLE IF EXISTS {self._fqtn_spark()}")
        sp.sql(S.spark_iceberg_ddl(
            self._fqtn_spark(), self.cols, self.cfg.schema.char_len,
            self.candidate.iceberg_format_version, self.candidate.table_properties))

    def load_round(self, round_idx: int, s3_uri: str) -> int:
        sp = self.ctx.spark
        local = f"file:///staging/round_{round_idx:02d}.parquet"
        sp.sql(f"CREATE OR REPLACE TEMPORARY VIEW src AS SELECT * FROM parquet.`{local}`")
        cols = ", ".join(f"`{c.name}`" for c in self.cols)
        fqtn = self._fqtn_spark()
        if round_idx == 0 or not self.supports_upsert():
            sp.sql(f"INSERT INTO {fqtn} ({cols}) SELECT {cols} FROM src")
        else:
            sp.sql(
                f"MERGE INTO {fqtn} t USING src s ON t.`{self.pk_col}` = s.`{self.pk_col}` "
                f"WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
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
        try:
            self.ctx.spark.sql(f"DROP TABLE IF EXISTS {self._fqtn_spark()}")
        except Exception:  # noqa: BLE001
            pass
