"""StarRocks client (MySQL protocol) — query engine for all candidates and writer
for Session A. Also creates the Polaris-backed Iceberg external catalog with metadata
caching disabled (project requirement: always read fresh)."""
from __future__ import annotations

import time

import pymysql

from ..config import BenchConfig


class StarRocksClient:
    def __init__(self, cfg: BenchConfig):
        self.cfg = cfg
        self.ep = cfg.endpoints

    def _conn(self, database: str | None = None):
        return pymysql.connect(
            host=self.ep.sr_host,
            port=self.ep.sr_query_port,
            user=self.ep.sr_user,
            password=self.ep.sr_password,
            database=database,
            autocommit=True,
            charset="utf8mb4",
            connect_timeout=30,
            read_timeout=3600,
            write_timeout=3600,
        )

    # ---- basic ops ------------------------------------------------------------
    def execute(self, sql: str, database: str | None = None) -> None:
        with self._conn(database) as c, c.cursor() as cur:
            cur.execute(sql)

    def execute_many(self, statements: list[str], database: str | None = None) -> None:
        with self._conn(database) as c, c.cursor() as cur:
            for s in statements:
                if s.strip():
                    cur.execute(s)

    def query(self, sql: str, database: str | None = None) -> list[tuple]:
        with self._conn(database) as c, c.cursor() as cur:
            cur.execute(sql)
            return list(cur.fetchall())

    def scalar(self, sql: str, database: str | None = None):
        rows = self.query(sql, database)
        return rows[0][0] if rows and rows[0] else None

    def wait_ready(self, timeout_s: int = 300) -> None:
        """Block until the FE answers and at least one BE is registered."""
        deadline = time.time() + timeout_s
        last = None
        while time.time() < deadline:
            try:
                if self.query("SHOW BACKENDS"):
                    return
            except Exception as e:  # noqa: BLE001
                last = e
            time.sleep(3)
        raise TimeoutError(f"StarRocks not ready after {timeout_s}s: {last}")

    # ---- Polaris (Iceberg REST) external catalog ------------------------------
    def ensure_iceberg_catalog(self, catalog_name: str = "ice") -> str:
        """Create (idempotently) an Iceberg REST external catalog -> Polaris, with
        metadata cache DISABLED. Returns the catalog name."""
        cfg, ep = self.cfg, self.ep
        props = {
            "type": "iceberg",
            "iceberg.catalog.type": "rest",
            "iceberg.catalog.uri": f"{ep.polaris_uri}/api/catalog",
            "iceberg.catalog.warehouse": ep.polaris_catalog,
            "iceberg.catalog.security": "oauth2",
            "iceberg.catalog.oauth2.credential": f"{ep.polaris_client_id}:{ep.polaris_client_secret}",
            "iceberg.catalog.oauth2.scope": ep.polaris_scope,
            # MinIO / S3
            "aws.s3.endpoint": cfg.s3.endpoint,
            "aws.s3.access_key": cfg.s3.access_key,
            "aws.s3.secret_key": cfg.s3.secret_key,
            "aws.s3.region": cfg.s3.region,
            "aws.s3.enable_path_style_access": "true",
            # Requirement: cache_enabled = false -> never mask load-to-query freshness.
            # `enable_iceberg_metadata_cache` is the current (3.5) key; the legacy
            # `enable_metadata_cache` is also set for older builds.
            "enable_iceberg_metadata_cache": "false",
            "enable_metadata_cache": "false",
        }
        prop_str = ",\n  ".join(f'"{k}" = "{v}"' for k, v in props.items())
        try:
            self.execute(
                f"CREATE EXTERNAL CATALOG IF NOT EXISTS `{catalog_name}` "
                f"PROPERTIES (\n  {prop_str}\n)")
        except pymysql.err.OperationalError as e:
            msg = str(e).lower()
            if "already exists" in msg:
                return catalog_name
            # An older build may reject an unknown cache key -> retry without legacy keys.
            if "enable_metadata_cache" in msg or "unknown" in msg or "invalid" in msg:
                props.pop("enable_metadata_cache", None)
                prop_str = ",\n  ".join(f'"{k}" = "{v}"' for k, v in props.items())
                self.execute(
                    f"CREATE EXTERNAL CATALOG IF NOT EXISTS `{catalog_name}` "
                    f"PROPERTIES (\n  {prop_str}\n)")
            else:
                raise
        return catalog_name

    def refresh_iceberg(self, catalog: str, db: str, table: str) -> None:
        """Best-effort metadata refresh so reads never see stale cached metadata."""
        for stmt in (
            f"REFRESH EXTERNAL TABLE `{catalog}`.`{db}`.`{table}`",
        ):
            try:
                self.execute(stmt)
            except Exception:  # noqa: BLE001
                pass

    # ---- staging Parquet (MinIO) via the FILES() table function ---------------
    def files_clause(self, s3_uri: str) -> str:
        cfg = self.cfg
        return (
            "FILES(\n"
            f'  "path" = "{s3_uri}",\n'
            '  "format" = "parquet",\n'
            f'  "aws.s3.endpoint" = "{cfg.s3.endpoint}",\n'
            f'  "aws.s3.access_key" = "{cfg.s3.access_key}",\n'
            f'  "aws.s3.secret_key" = "{cfg.s3.secret_key}",\n'
            f'  "aws.s3.region" = "{cfg.s3.region}",\n'
            '  "aws.s3.enable_path_style_access" = "true"\n'
            ")"
        )

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:mysql://{self.ep.sr_host}:{self.ep.sr_query_port}"

    @property
    def load_url(self) -> str:
        return f"{self.ep.sr_host}:{self.ep.sr_http_port}"
