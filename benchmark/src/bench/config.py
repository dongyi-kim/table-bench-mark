"""Configuration loading: merges YAML config with environment overrides.

All tunables live in benchmark/config/*.yaml and the environment (.env). Code reads
strongly-typed dataclasses from here — no magic numbers elsewhere.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


def _env(name: str, default: str | None = None) -> str | None:
    val = os.environ.get(name)
    return val if val not in (None, "") else default


# ──────────────────────────────────────────────────────────────────────────────
# Typed config sections
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class SchemaConfig:
    double_cols: int
    int_cols: int
    char_cols: int
    char_len: int
    pk_column: str
    round_column: str


@dataclass(frozen=True)
class WorkloadConfig:
    initial_rows: int
    rows_per_round: int
    rounds: int
    update_ratio: float
    query_recent_rounds: int
    query_repeats: int


@dataclass(frozen=True)
class Warehouse:
    bucket: str
    iceberg_prefix: str
    paimon_prefix: str
    staging_prefix: str = "staging"


@dataclass(frozen=True)
class S3Settings:
    endpoint: str
    region: str
    access_key: str
    secret_key: str


@dataclass(frozen=True)
class Endpoints:
    sr_host: str
    sr_query_port: int
    sr_http_port: int
    sr_user: str
    sr_password: str
    flink_sql_gateway_url: str
    flink_rest_url: str
    spark_connect_url: str
    polaris_uri: str
    polaris_catalog: str
    polaris_client_id: str
    polaris_client_secret: str
    polaris_scope: str


@dataclass(frozen=True)
class Candidate:
    name: str
    adapter: str
    write_engine: str
    table_format: str
    mode: str
    catalog: str
    table_properties: dict = field(default_factory=dict)
    capabilities: dict = field(default_factory=dict)
    iceberg_format_version: int = 3
    catalog_fallback: str | None = None
    raw: dict = field(default_factory=dict)


@dataclass
class BenchConfig:
    seed: int
    schema: SchemaConfig
    workload: WorkloadConfig
    smoke: dict
    warehouse: Warehouse
    s3: S3Settings
    endpoints: Endpoints
    query_engine: str        # StarRocks version label, e.g. "starrocks-4.1.1"
    compaction_modes: list[str]
    read_engines: list[str]
    iceberg_table_defaults: dict
    freshness_timeout_s: float
    freshness_poll_s: float
    settle_s: float          # idle wait before each measured step's timer (quiescence)
    candidates: dict[str, Candidate]

    # ---- convenience ----------------------------------------------------------
    def staging_s3_uri(self, filename: str) -> str:
        return f"s3://{self.warehouse.bucket}/{self.warehouse.staging_prefix}/{filename}"

    def with_smoke(self) -> "BenchConfig":
        """Return a copy whose workload is overridden by the `smoke` profile."""
        w = self.workload
        s = self.smoke
        smoke_workload = WorkloadConfig(
            initial_rows=int(s.get("initial_rows", w.initial_rows)),
            rows_per_round=int(s.get("rows_per_round", w.rows_per_round)),
            rounds=int(s.get("rounds", w.rounds)),
            update_ratio=float(s.get("update_ratio", w.update_ratio)),
            query_recent_rounds=int(s.get("query_recent_rounds", w.query_recent_rounds)),
            query_repeats=int(s.get("query_repeats", w.query_repeats)),
        )
        self.workload = smoke_workload
        return self


# ──────────────────────────────────────────────────────────────────────────────
# Loading
# ──────────────────────────────────────────────────────────────────────────────
def _load_yaml(path: Path) -> dict:
    with path.open() as fh:
        return yaml.safe_load(fh) or {}


def load_config(config_dir: Path = CONFIG_DIR) -> BenchConfig:
    bench = _load_yaml(config_dir / "benchmark.yaml")

    sc = bench["schema"]
    schema = SchemaConfig(
        double_cols=sc["double_cols"],
        int_cols=sc["int_cols"],
        char_cols=sc["char_cols"],
        char_len=sc["char_len"],
        pk_column=sc["pk_column"],
        round_column=sc["round_column"],
    )

    wl = bench["workload"]
    workload = WorkloadConfig(
        initial_rows=int(_env("BENCH_INITIAL_ROWS", wl["initial_rows"])),
        rows_per_round=int(_env("BENCH_ROWS_PER_ROUND", wl["rows_per_round"])),
        rounds=int(_env("BENCH_ROUNDS", wl["rounds"])),
        update_ratio=float(_env("BENCH_UPDATE_RATIO", wl["update_ratio"])),
        query_recent_rounds=int(wl["query_recent_rounds"]),
        query_repeats=int(wl["query_repeats"]),
    )

    wh = bench["warehouse"]
    warehouse = Warehouse(
        bucket=_env("WAREHOUSE_BUCKET", wh["bucket"]),
        iceberg_prefix=wh["iceberg_prefix"],
        paimon_prefix=wh["paimon_prefix"],
    )

    s3 = S3Settings(
        endpoint=_env("MINIO_ENDPOINT", "http://minio:9000"),
        region=_env("MINIO_REGION", "us-east-1"),
        access_key=_env("MINIO_ROOT_USER", "admin"),
        secret_key=_env("MINIO_ROOT_PASSWORD", "password123"),
    )

    endpoints = Endpoints(
        sr_host=_env("SR_FE_HOST", "starrocks"),
        sr_query_port=int(_env("SR_QUERY_PORT", "9030")),
        sr_http_port=int(_env("SR_HTTP_PORT", "8030")),
        sr_user=_env("SR_USER", "root"),
        sr_password=_env("SR_PASSWORD", "") or "",
        flink_sql_gateway_url=_env("FLINK_SQL_GATEWAY_URL", "http://flink-sql-gateway:8083"),
        flink_rest_url=_env("FLINK_REST_URL", "http://flink-jobmanager:8081"),
        spark_connect_url=_env("SPARK_CONNECT_URL", "sc://spark:15002"),
        polaris_uri=_env("POLARIS_URI", "http://polaris:8181"),
        polaris_catalog=_env("POLARIS_CATALOG_NAME", "polaris_catalog"),
        polaris_client_id=_env("POLARIS_CLIENT_ID", "root"),
        polaris_client_secret=_env("POLARIS_CLIENT_SECRET", "s3cr3t"),
        polaris_scope=_env("POLARIS_PRINCIPAL_ROLE", "PRINCIPAL_ROLE:ALL"),
    )

    candidates: dict[str, Candidate] = {}
    for path in sorted((config_dir / "candidates").glob("*.yaml")):
        c = _load_yaml(path)
        candidates[c["name"]] = Candidate(
            name=c["name"],
            adapter=c["adapter"],
            write_engine=c["write_engine"],
            table_format=c["table_format"],
            mode=c["mode"],
            catalog=c["catalog"],
            table_properties=c.get("table_properties", {}) or {},
            capabilities=c.get("capabilities", {}) or {},
            iceberg_format_version=int(c.get("iceberg_format_version", 3)),
            catalog_fallback=c.get("catalog_fallback"),
            raw=c,
        )

    # Query-engine label: explicit override, else derived from the StarRocks image version.
    query_engine = _env("BENCH_QUERY_ENGINE") or f"starrocks-{_env('STARROCKS_VERSION', 'unknown')}"

    return BenchConfig(
        seed=int(_env("BENCH_SEED", bench["seed"])),
        schema=schema,
        workload=workload,
        smoke=bench.get("smoke", {}),
        warehouse=warehouse,
        s3=s3,
        endpoints=endpoints,
        query_engine=query_engine,
        compaction_modes=list(bench.get("compaction_modes", ["none"])),
        read_engines=list(bench.get("read_engines", ["starrocks"])),
        iceberg_table_defaults=dict(bench.get("iceberg_table_defaults", {})),
        freshness_timeout_s=float(bench.get("freshness", {}).get("timeout_s", 8)),
        freshness_poll_s=float(bench.get("freshness", {}).get("poll_s", 0.2)),
        settle_s=float(_env("BENCH_SETTLE_S", bench.get("measurement", {}).get("settle_s", 1.0))),
        candidates=candidates,
    )
