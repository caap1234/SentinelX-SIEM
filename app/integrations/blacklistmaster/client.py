from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx


@dataclass(frozen=True)
class BlacklistMasterIP:
    ip: str
    group_name: str
    ip_description: str
    last_test_date_utc: str
    blacklist_cnt: int
    new_blacklist_cnt: int
    severity: str
    blacklists: List[Dict[str, Any]]


class BlacklistMasterClient:
    def __init__(self, *, base_url: str, api_key: str, timeout_s: float = 20.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._timeout = httpx.Timeout(timeout_s)

    def fetch_ipbl(self) -> List[BlacklistMasterIP]:
        """
        GET /restapi/v1/ipbl?apikey=...
        Retorna estado por IP con lista de blacklists.
        """
        url = f"{self._base_url}/restapi/v1/ipbl"
        params = {"apikey": self._api_key}

        with httpx.Client(timeout=self._timeout) as client:
            r = client.get(url, params=params)
            r.raise_for_status()
            data = r.json()

        if not isinstance(data, dict) or data.get("status") != "SUCCESS":
            raise RuntimeError(f"BlacklistMaster API error: {data}")

        ips = data.get("IPs", [])
        out: List[BlacklistMasterIP] = []

        for item in ips:
            if not isinstance(item, dict):
                continue

            out.append(
                BlacklistMasterIP(
                    ip=str(item.get("ip", "")).strip(),
                    group_name=str(item.get("group_name", "")).strip(),
                    ip_description=str(item.get("ip_description", "")).strip(),
                    last_test_date_utc=str(item.get("last_test_date_UTC", "")).strip(),
                    blacklist_cnt=int(str(item.get("blacklist_cnt", "0") or "0")),
                    new_blacklist_cnt=int(str(item.get("new_blacklist_cnt", "0") or "0")),
                    severity=str(item.get("severity", "")).strip(),
                    blacklists=list(item.get("blacklists", []) or []),
                )
            )

        return out

