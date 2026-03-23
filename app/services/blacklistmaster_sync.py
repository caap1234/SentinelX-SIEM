from __future__ import annotations

import hashlib
import ipaddress
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.integrations.blacklistmaster.client import (
    BlacklistMasterClient,
    BlacklistMasterIP,
)
from app.models.alert import Alert
from app.models.user_setting import UserSetting

_SOURCE = "blacklistmaster"
_EVENT_TYPE = "intel"

_RULES_BY_CATEGORY: Dict[str, Tuple[str, str]] = {
    "dedicated": ("BLM-001", "IP listed in RBL (Dedicated/VPS)"),
    "shared": ("BLM-002", "IP listed in RBL (Shared Server)"),
    "pmg": ("BLM-003", "IP listed in RBL (PMG Relay)"),
}


@dataclass(frozen=True)
class InventoryMatch:
    category: str  # shared | pmg | dedicated
    server: Optional[str]


@dataclass(frozen=True)
class IgnoreList:
    ips: Set[str]
    cidrs: Tuple[ipaddress._BaseNetwork, ...]  # type: ignore[attr-defined]


def _load_inventory_map(json_path: Path) -> Dict[str, str]:
    """
    Devuelve map ip -> server_name
    Estructura esperada:
    { "servers": { "svdb057": { "server_ips": ["1.2.3.4"] } } }
    """
    if not json_path.exists():
        return {}

    data = json.loads(json_path.read_text(encoding="utf-8"))
    servers = data.get("servers", {}) if isinstance(data, dict) else {}
    out: Dict[str, str] = {}

    if not isinstance(servers, dict):
        return out

    for server_name, payload in servers.items():
        if not isinstance(payload, dict):
            continue
        ips = payload.get("server_ips", [])
        if not isinstance(ips, list):
            continue
        for ip in ips:
            ip_s = str(ip).strip()
            if ip_s:
                out[ip_s] = str(server_name)

    return out


def _parse_csv_env(name: str) -> List[str]:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return []
    return [p.strip() for p in raw.split(",") if p.strip()]


def _load_ignore_list(json_path: Path) -> IgnoreList:
    """
    Carga ignore list desde:
      - JSON: { "ips": [...], "cidrs": [...] }
      - ENV:
          BLACKLISTMASTER_IGNORE_IPS
          BLACKLISTMASTER_IGNORE_CIDRS
    """
    ips: Set[str] = set()
    cidrs: List[ipaddress._BaseNetwork] = []  # type: ignore[attr-defined]

    # JSON
    if json_path.exists():
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                for ip in data.get("ips", []) or []:
                    ip_s = str(ip).strip()
                    if ip_s:
                        ips.add(ip_s)

                for c in data.get("cidrs", []) or []:
                    c_s = str(c).strip()
                    if not c_s:
                        continue
                    try:
                        cidrs.append(ipaddress.ip_network(c_s, strict=False))
                    except Exception:
                        continue
        except Exception:
            pass

    # ENV
    for ip_s in _parse_csv_env("BLACKLISTMASTER_IGNORE_IPS"):
        ips.add(ip_s)

    for c_s in _parse_csv_env("BLACKLISTMASTER_IGNORE_CIDRS"):
        try:
            cidrs.append(ipaddress.ip_network(c_s, strict=False))
        except Exception:
            continue

    uniq = {str(n): n for n in cidrs}
    return IgnoreList(ips=ips, cidrs=tuple(uniq.values()))


def _is_ignored_ip(ip: str, ignore: IgnoreList) -> bool:
    if ip in ignore.ips:
        return True
    try:
        addr = ipaddress.ip_address(ip)
    except Exception:
        return False
    for net in ignore.cidrs:
        if addr in net:
            return True
    return False


def _classify_ip(ip: str, *, shared_map: Dict[str, str], pmg_map: Dict[str, str]) -> InventoryMatch:
    if ip in shared_map:
        return InventoryMatch(category="shared", server=shared_map[ip])
    if ip in pmg_map:
        return InventoryMatch(category="pmg", server=pmg_map[ip])
    return InventoryMatch(category="dedicated", server=None)


def _severity_for(category: str) -> int:
    if category == "shared":
        return 28
    if category == "pmg":
        return 26
    return 14  # dedicated / vps


def _blacklists_hash(blacklists: List[Dict[str, Any]]) -> str:
    names: List[str] = []
    for b in blacklists:
        if isinstance(b, dict):
            names.append(str(b.get("blacklist", "")).strip())
    names = sorted([n for n in names if n])
    raw = "|".join(names).encode("utf-8", errors="ignore")
    return hashlib.sha256(raw).hexdigest()


def _get_last_alert_for_ip(db: Session, group_key: str) -> Optional[Alert]:
    return (
        db.query(Alert)
        .filter(Alert.group_key == group_key)
        .filter(Alert.source == _SOURCE)
        .order_by(Alert.triggered_at.desc())
        .limit(1)
        .one_or_none()
    )


def _should_create_alert(last_alert: Optional[Alert], new_hash: str) -> bool:
    if last_alert is None:
        return True
    try:
        old_hash = (last_alert.evidence or {}).get("blacklists_hash")
    except Exception:
        old_hash = None
    return old_hash != new_hash


def _create_alert(
    db: Session,
    *,
    ip: str,
    match: InventoryMatch,
    bm: BlacklistMasterIP,
    blacklists_hash: str,
) -> Alert:
    now = datetime.now(timezone.utc)

    rule_code, rule_desc = _RULES_BY_CATEGORY.get(
        match.category, _RULES_BY_CATEGORY["dedicated"]
    )
    rule_name = f"{rule_code} | {rule_desc}"
    group_key = f"{rule_code}|{ip}"

    severity = _severity_for(match.category)

    alert = Alert(
        rule_id=None,
        rule_name=rule_name,
        severity=severity,
        server=match.server,
        source=_SOURCE,
        event_type=_EVENT_TYPE,
        group_key=group_key,
        triggered_at=now,
        metrics={
            "blacklist_cnt": bm.blacklist_cnt,
            "new_blacklist_cnt": bm.new_blacklist_cnt,
        },
        evidence={
            "ip": ip,
            "category": match.category,
            "provider_severity": bm.severity,
            "group_name": bm.group_name,
            "ip_description": bm.ip_description,
            "last_test_date_utc": bm.last_test_date_utc,
            "blacklists": bm.blacklists,
            "blacklists_hash": blacklists_hash,
        },
        status="open",
    )

    db.add(alert)
    db.flush()
    return alert


def _iter_users_with_alert_email(db: Session) -> List[int]:
    rows = db.query(UserSetting).filter(UserSetting.key == "alert_email").all()
    user_ids: List[int] = []

    for r in rows:
        try:
            data = json.loads(r.value or "")
            to_email = data.get("to_email")
            if to_email:
                user_ids.append(int(r.user_id))
        except Exception:
            continue

    return sorted(set(user_ids))


def _notify_alert_created(db: Session, alert: Alert) -> None:
    from app.services.notification_dispatch import notify_on_alert_created

    for user_id in _iter_users_with_alert_email(db):
        try:
            notify_on_alert_created(db=db, alert=alert, user_id=user_id)
        except Exception:
            continue

    db.flush()


def run_blacklistmaster_sync() -> Dict[str, Any]:
    api_key = os.getenv("BLACKLISTMASTER_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Missing env BLACKLISTMASTER_API_KEY")

    base_url = os.getenv(
        "BLACKLISTMASTER_BASE_URL", "https://www.blacklistmaster.com"
    ).strip()

    config_dir = Path(__file__).resolve().parents[1] / "config"
    shared_path = config_dir / "blacklistmaster_shared.json"
    pmg_path = config_dir / "blacklistmaster_pmg.json"
    ignore_path = config_dir / "blacklistmaster_ignore.json"

    shared_map = _load_inventory_map(shared_path)
    pmg_map = _load_inventory_map(pmg_path)
    ignore = _load_ignore_list(ignore_path)

    client = BlacklistMasterClient(base_url=base_url, api_key=api_key)
    ips = client.fetch_ipbl()

    created = 0
    skipped = 0
    ignored = 0
    total_listed = 0
    emailed = 0

    with SessionLocal() as db:
        for bm in ips:
            ip = bm.ip
            if not ip:
                continue

            if _is_ignored_ip(ip, ignore):
                ignored += 1
                continue

            if bm.blacklist_cnt <= 0:
                continue

            total_listed += 1

            match = _classify_ip(ip, shared_map=shared_map, pmg_map=pmg_map)
            bl_hash = _blacklists_hash(bm.blacklists)

            rule_code, _ = _RULES_BY_CATEGORY.get(
                match.category, _RULES_BY_CATEGORY["dedicated"]
            )
            group_key = f"{rule_code}|{ip}"

            last_alert = _get_last_alert_for_ip(db, group_key)
            if not _should_create_alert(last_alert, bl_hash):
                skipped += 1
                continue

            alert = _create_alert(
                db, ip=ip, match=match, bm=bm, blacklists_hash=bl_hash
            )

            _notify_alert_created(db, alert)
            emailed += 1
            db.commit()
            created += 1

    return {
        "ok": True,
        "created_alerts": created,
        "emailed_alerts": emailed,
        "skipped_same_state": skipped,
        "ignored_ips": ignored,
        "total_listed_ips": total_listed,
        "total_ips_returned": len(ips),
    }

