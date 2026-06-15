"""Spark client over Spark Connect — the Session A Iceberg writer.

A single remote SparkSession is reused so the JVM stays warm and load timings reflect
actual work, not session startup. The Iceberg REST (Polaris) catalog is configured
server-side via spark-defaults.conf, so we only issue SQL here.
"""
from __future__ import annotations

import time

from ..config import BenchConfig


class SparkClient:
    def __init__(self, cfg: BenchConfig):
        self.cfg = cfg
        self.remote = cfg.endpoints.spark_connect_url
        self._spark = None

    def session(self):
        if self._spark is None:
            from pyspark.sql import SparkSession  # imported lazily (heavy dep)
            self._spark = SparkSession.builder.remote(self.remote).getOrCreate()
        return self._spark

    def sql(self, statement: str):
        """Execute a statement to completion. For commands (CREATE/INSERT/MERGE) Spark
        Connect executes eagerly; .collect() forces materialization of any result."""
        df = self.session().sql(statement)
        try:
            df.collect()
        except Exception:  # noqa: BLE001 - commands may return no schema
            pass
        return df

    def scalar(self, statement: str):
        rows = self.session().sql(statement).collect()
        return rows[0][0] if rows else None

    def wait_ready(self, timeout_s: int = 240) -> None:
        deadline = time.time() + timeout_s
        last = None
        while time.time() < deadline:
            try:
                self.scalar("SELECT 1")
                return
            except Exception as e:  # noqa: BLE001
                last = e
            time.sleep(5)
        raise TimeoutError(f"Spark Connect not ready after {timeout_s}s: {last}")
