"""The 100-column benchmark schema and per-engine type mappings.

Column budget (total 100):
  - pk_id     : BIGINT  (primary key)          ┐ drawn from the 10 integer columns
  - round_id  : INT     (ingest-round marker)  ┘
  - i2..i9    : INT     (8 more integers)
  - d0..d79   : DOUBLE  (80 reals)
  - c0..c9    : CHAR(N) (10 fixed-length strings)
"""
from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

from .config import SchemaConfig


@dataclass(frozen=True)
class Column:
    name: str
    kind: str  # 'bigint' | 'int' | 'double' | 'char'
    is_pk: bool = False
    is_round: bool = False


def build_columns(sc: SchemaConfig) -> list[Column]:
    cols: list[Column] = [
        Column(sc.pk_column, "bigint", is_pk=True),
        Column(sc.round_column, "int", is_round=True),
    ]
    # remaining integers
    for i in range(2, sc.int_cols):
        cols.append(Column(f"i{i}", "int"))
    for i in range(sc.double_cols):
        cols.append(Column(f"d{i}", "double"))
    for i in range(sc.char_cols):
        cols.append(Column(f"c{i}", "char"))
    assert len(cols) == sc.double_cols + sc.int_cols + sc.char_cols, "column budget mismatch"
    return cols


# ──────────────────────────────────────────────────────────────────────────────
# Type mappings
# ──────────────────────────────────────────────────────────────────────────────
def arrow_schema(cols: list[Column], char_len: int) -> pa.Schema:
    fields = []
    for c in cols:
        if c.kind == "bigint":
            fields.append(pa.field(c.name, pa.int64()))
        elif c.kind == "int":
            fields.append(pa.field(c.name, pa.int32()))
        elif c.kind == "double":
            fields.append(pa.field(c.name, pa.float64()))
        elif c.kind == "char":
            fields.append(pa.field(c.name, pa.string()))
        else:
            raise ValueError(c.kind)
    return pa.schema(fields)


def _sr_type(c: Column, char_len: int) -> str:
    return {
        "bigint": "BIGINT",
        "int": "INT",
        "double": "DOUBLE",
        "char": f"CHAR({char_len})",
    }[c.kind]


def _iceberg_sr_type(c: Column, char_len: int) -> str:
    # StarRocks dialect for Iceberg external tables (no fixed CHAR -> VARCHAR/STRING).
    return {
        "bigint": "BIGINT",
        "int": "INT",
        "double": "DOUBLE",
        "char": f"VARCHAR({char_len})",
    }[c.kind]


def _flink_type(c: Column, char_len: int, nullable_pk: bool = False) -> str:
    base = {
        "bigint": "BIGINT",
        "int": "INT",
        "double": "DOUBLE",
        "char": "STRING",
    }[c.kind]
    if c.is_pk and not nullable_pk:
        base += " NOT NULL"
    return base


# ──────────────────────────────────────────────────────────────────────────────
# DDL builders
# ──────────────────────────────────────────────────────────────────────────────
def starrocks_internal_ddl(db: str, table: str, cols: list[Column], char_len: int,
                           props: dict) -> str:
    """PRIMARY KEY model table (native upsert)."""
    col_defs = ",\n  ".join(f"`{c.name}` {_sr_type(c, char_len)}" for c in cols)
    pk = next(c for c in cols if c.is_pk)
    prop_str = ",\n  ".join(f'"{k}" = "{v}"' for k, v in props.items())
    return (
        f"CREATE TABLE `{db}`.`{table}` (\n  {col_defs}\n)\n"
        f"PRIMARY KEY (`{pk.name}`)\n"
        f"DISTRIBUTED BY HASH(`{pk.name}`)\n"
        + (f"PROPERTIES (\n  {prop_str}\n)" if props else "")
        + ";"
    )


def iceberg_sr_ddl(catalog: str, db: str, table: str, cols: list[Column], char_len: int,
                   format_version: int, table_props: dict) -> str:
    """Iceberg table created through StarRocks against the Polaris catalog."""
    col_defs = ",\n  ".join(f"`{c.name}` {_iceberg_sr_type(c, char_len)}" for c in cols)
    props = {"format-version": str(format_version), **table_props}
    prop_str = ",\n  ".join(f'"{k}" = "{v}"' for k, v in props.items())
    return (
        f"CREATE TABLE `{catalog}`.`{db}`.`{table}` (\n  {col_defs}\n)\n"
        f"PROPERTIES (\n  {prop_str}\n);"
    )


def _spark_type(c: Column, char_len: int) -> str:
    return {
        "bigint": "BIGINT",
        "int": "INT",
        "double": "DOUBLE",
        "char": "STRING",
    }[c.kind]


def spark_iceberg_ddl(fqtn: str, cols: list[Column], char_len: int,
                      format_version: int, table_props: dict) -> str:
    """Iceberg table created by Spark (USING iceberg). pk_id is the identifier field
    used for MERGE upsert; declared NOT NULL so v3 equality/merge works."""
    defs = []
    for c in cols:
        t = _spark_type(c, char_len)
        if c.is_pk:
            t += " NOT NULL"
        defs.append(f"`{c.name}` {t}")
    col_defs = ",\n  ".join(defs)
    props = {"format-version": str(format_version), **table_props}
    prop_str = ",\n  ".join(f"'{k}'='{v}'" for k, v in props.items())
    return (
        f"CREATE TABLE {fqtn} (\n  {col_defs}\n)\n"
        f"USING iceberg\n"
        f"TBLPROPERTIES (\n  {prop_str}\n)"
    )


def flink_iceberg_ddl(table: str, cols: list[Column], char_len: int,
                      format_version: int, table_props: dict, upsert: bool) -> str:
    pk = next(c for c in cols if c.is_pk)
    col_defs = ",\n  ".join(f"`{c.name}` {_flink_type(c, char_len)}" for c in cols)
    props = {"format-version": str(format_version), **table_props}
    if upsert:
        props["write.upsert.enabled"] = "true"
    prop_str = ",\n  ".join(f"'{k}'='{v}'" for k, v in props.items())
    # `table` is the caller-supplied, already-quoted identifier (do not re-quote).
    return (
        f"CREATE TABLE IF NOT EXISTS {table} (\n  {col_defs},\n"
        f"  PRIMARY KEY (`{pk.name}`) NOT ENFORCED\n)\n"
        f"WITH (\n  {prop_str}\n);"
    )


def flink_paimon_ddl(table: str, cols: list[Column], char_len: int,
                     table_props: dict) -> str:
    pk = next(c for c in cols if c.is_pk)
    col_defs = ",\n  ".join(f"`{c.name}` {_flink_type(c, char_len)}" for c in cols)
    prop_str = ",\n  ".join(f"'{k}'='{v}'" for k, v in table_props.items())
    return (
        f"CREATE TABLE IF NOT EXISTS {table} (\n  {col_defs},\n"
        f"  PRIMARY KEY (`{pk.name}`) NOT ENFORCED\n)\n"
        + (f"WITH (\n  {prop_str}\n)" if table_props else "")
        + ";"
    )


def flink_filesystem_source_ddl(table: str, cols: list[Column], char_len: int,
                                s3_path: str) -> str:
    """A Flink source table over a single staging Parquet object in MinIO."""
    col_defs = ",\n  ".join(f"`{c.name}` {_flink_type(c, char_len, nullable_pk=True)}" for c in cols)
    return (
        f"CREATE TABLE IF NOT EXISTS {table} (\n  {col_defs}\n)\n"
        f"WITH (\n"
        f"  'connector'='filesystem',\n"
        f"  'path'='{s3_path}',\n"
        f"  'format'='parquet'\n);"
    )


def flink_starrocks_sink_ddl(table: str, cols: list[Column], char_len: int,
                             jdbc_url: str, load_url: str, db: str, sr_table: str,
                             user: str, password: str) -> str:
    """Flink table backed by the StarRocks sink connector -> internal PK table."""
    pk = next(c for c in cols if c.is_pk)
    col_defs = ",\n  ".join(f"`{c.name}` {_flink_type(c, char_len)}" for c in cols)
    return (
        f"CREATE TABLE IF NOT EXISTS {table} (\n  {col_defs},\n"
        f"  PRIMARY KEY (`{pk.name}`) NOT ENFORCED\n)\n"
        f"WITH (\n"
        f"  'connector'='starrocks',\n"
        f"  'jdbc-url'='{jdbc_url}',\n"
        f"  'load-url'='{load_url}',\n"
        f"  'database-name'='{db}',\n"
        f"  'table-name'='{sr_table}',\n"
        f"  'username'='{user}',\n"
        f"  'password'='{password}',\n"
        f"  'sink.properties.format'='json',\n"
        f"  'sink.properties.strip_outer_array'='true'\n);"
    )


def column_names(cols: list[Column]) -> list[str]:
    return [c.name for c in cols]
