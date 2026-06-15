"""Thin Polaris REST helper. Catalog/role bootstrap happens in
infra/polaris/bootstrap.sh at stack start-up; this is used by the runner only to
verify connectivity and (optionally) fetch a token for diagnostics."""
from __future__ import annotations

import requests

from ..config import BenchConfig


class PolarisClient:
    def __init__(self, cfg: BenchConfig):
        self.ep = cfg.endpoints
        self.uri = self.ep.polaris_uri.rstrip("/")

    def token(self) -> str:
        r = requests.post(
            f"{self.uri}/api/catalog/v1/oauth/tokens",
            data={
                "grant_type": "client_credentials",
                "client_id": self.ep.polaris_client_id,
                "client_secret": self.ep.polaris_client_secret,
                "scope": "PRINCIPAL_ROLE:ALL",
            },
            timeout=30,
        )
        r.raise_for_status()
        return r.json()["access_token"]

    def catalog_exists(self) -> bool:
        try:
            tok = self.token()
            r = requests.get(f"{self.uri}/api/management/v1/catalogs",
                             headers={"Authorization": f"Bearer {tok}"}, timeout=30)
            r.raise_for_status()
            names = [c.get("name") for c in r.json().get("catalogs", [])]
            return self.ep.polaris_catalog in names
        except Exception:  # noqa: BLE001
            return False

    @property
    def rest_uri(self) -> str:
        return f"{self.uri}/api/catalog"
