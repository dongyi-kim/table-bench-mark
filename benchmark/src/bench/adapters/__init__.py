"""Adapter registry. Importing this package registers all built-in adapters."""
from __future__ import annotations

from .base import REGISTRY, Adapter, AdapterContext, UnsupportedOperation  # noqa: F401

# Import side-effect: each module registers its adapter class.
from . import iceberg_spark  # noqa: F401,E402  (Spark writes Iceberg; StarRocks queries)


def get_adapter(name: str):
    if name not in REGISTRY:
        raise KeyError(f"unknown adapter '{name}'. registered: {sorted(REGISTRY)}")
    return REGISTRY[name]
