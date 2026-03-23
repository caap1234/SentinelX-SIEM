# app/parsing/sar_stats.py
from __future__ import annotations

import re
from datetime import datetime, date, timedelta
from typing import Optional

from app.core.timeutils import SERVER_TZ, parse_any_timestamp_to_utc
from app.parsing.base import LogParser
from app.parsing.types import ParsedEvent

# Control lines (tu shipper):
# SAR_DATE=2025-12-15
# SAR_MODE=-q
SAR_DATE_RE = re.compile(r"^(?:#)?SAR_DATE=(?P<d>\d{4}-\d{2}-\d{2})\s*$")
SAR_MODE_RE = re.compile(r"^(?:#)?SAR_MODE=(?P<m>-\w+)\s*$")

CPU_COUNT_RE = re.compile(r"\((?P<n>\d+)\s+CPU\)")
TIME_RE = r"(?P<time>\d{1,2}:\d{2}:\d{2})(?:\s*(?P<ampm>AM|PM))?"

Q_RE = re.compile(
    rf"^{TIME_RE}\s+"
    r"(?P<runq>\d+(\.\d+)?)\s+"
    r"(?P<plist>\d+(\.\d+)?)\s+"
    r"(?P<ld1>\d+(\.\d+)?)\s+"
    r"(?P<ld5>\d+(\.\d+)?)\s+"
    r"(?P<ld15>\d+(\.\d+)?)\s+"
    r"(?P<blocked>\d+(\.\d+)?)\s*$"
)

# sar -r (nuevo / RHEL9+): time kbmemfree kbavail kbmemused %memused ...
R_RE_WITH_AVAIL = re.compile(
    rf"^{TIME_RE}\s+"
    r"(?P<kbfree>\d+)\s+"
    r"(?P<kbavail>\d+)\s+"
    r"(?P<kbused>\d+)\s+"
    r"(?P<memused_pct>\d+(\.\d+)?)"
    r".*$"
)

# sar -r (viejo): time kbmemfree kbmemused %memused ...
R_RE_NO_AVAIL = re.compile(
    rf"^{TIME_RE}\s+"
    r"(?P<kbfree>\d+)\s+"
    r"(?P<kbused>\d+)\s+"
    r"(?P<memused_pct>\d+(\.\d+)?)"
    r".*$"
)

D_RE = re.compile(
    rf"^{TIME_RE}\s+"
    r"(?P<dev>\S+)\s+"
    r"(?P<tps>\d+(\.\d+)?)\s+"
    r"(?P<rest>.*?)(?P<util>\d+(\.\d+)?)\s*$"
)


def _to_float(s: str) -> float:
    return float((s or "0").replace(",", "."))


def _normalize_sar_date(d: date) -> date:
    """
    Protege contra shipper que setea SAR_DATE en UTC (date -u) mientras sar imprime hora local.
    Si SAR_DATE viene adelantado vs fecha local del server en el momento del parseo, lo bajamos 1 día.
    """
    today_local = datetime.now(SERVER_TZ).date()
    if d > today_local:
        # típico: d == today_local + 1 por UTC; corregimos una vez
        return d - timedelta(days=1)
    return d


def _parse_ts(hms: str, ampm: Optional[str], d: date) -> datetime:
    """
    sar imprime hora local (sin tz). Convertimos a UTC usando timeutils.parse_any_timestamp_to_utc,
    que asume SERVER_TZ (America/Mexico_City) cuando el valor no trae zona.
    """
    hh, mm, ss = [int(x) for x in hms.split(":")]
    if ampm:
        ampm = ampm.upper()
        if ampm == "PM" and hh != 12:
            hh += 12
        if ampm == "AM" and hh == 12:
            hh = 0

    ts_local_str = f"{d.isoformat()} {hh:02d}:{mm:02d}:{ss:02d}"
    return parse_any_timestamp_to_utc(ts_local_str)


class SarStatsParser(LogParser):
    source = "SAR_STATS"

    def __init__(self) -> None:
        self._current_date: Optional[date] = None
        self._mode: Optional[str] = None
        self._cpu_count: Optional[int] = None

    def parse_line(
        self,
        line: str,
        server: str,
        *,
        log_upload_id: Optional[int] = None,
    ) -> Optional[ParsedEvent]:
        line = (line or "").strip()
        if not line:
            return None

        md = SAR_DATE_RE.match(line)
        if md:
            d = datetime.strptime(md.group("d"), "%Y-%m-%d").date()
            self._current_date = _normalize_sar_date(d)
            return None

        mm = SAR_MODE_RE.match(line)
        if mm:
            self._mode = (mm.group("m") or "").strip()
            return None

        mcc = CPU_COUNT_RE.search(line)
        if mcc:
            try:
                self._cpu_count = int(mcc.group("n"))
            except Exception:
                pass

        # ignora headers
        if line.startswith("Linux ") or line.startswith("Average:") or "LINUX RESTART" in line:
            return None
        if "runq-sz" in line and "ldavg-1" in line:
            return None

        # header sar -r (para no intentar parsear la fila de títulos)
        low = line.lower()
        if "kbmemfree" in low and "%memused" in low:
            return None

        if self._current_date is None:
            return None

        mode = (self._mode or "").lower().strip()

        # ---- sar -q ----
        mq = Q_RE.match(line)
        if mq and (mode in ("", "-q")):
            ts = _parse_ts(mq.group("time"), mq.group("ampm"), self._current_date)

            ld1 = _to_float(mq.group("ld1"))
            ld5 = _to_float(mq.group("ld5"))
            ld15 = _to_float(mq.group("ld15"))
            runq = _to_float(mq.group("runq"))
            blocked = _to_float(mq.group("blocked"))
            cpu_n = int(self._cpu_count or 0)

            extra = {
                "event_type": "metric",
                "metric": {
                    "family": "sar",
                    "name": "load",
                    "cpu_count": cpu_n,
                    "runq_sz": runq,
                    "plist_sz": _to_float(mq.group("plist")),
                    "blocked": blocked,
                    "ldavg_1": ld1,
                    "ldavg_5": ld5,
                    "ldavg_15": ld15,
                },
            }
            if cpu_n > 0:
                extra["metric"].update(
                    {
                        "ldavg_1_per_cpu": ld1 / cpu_n,
                        "ldavg_5_per_cpu": ld5 / cpu_n,
                        "ldavg_15_per_cpu": ld15 / cpu_n,
                    }
                )

            return ParsedEvent(
                timestamp_utc=ts,
                server=server,
                source=self.source,
                service="INFRA",
                message="sar -q load",
                extra=extra,
                log_upload_id=log_upload_id,
            )

        # ---- sar -r ----
        mr = R_RE_WITH_AVAIL.match(line)
        used_with_avail = True
        if not mr:
            mr = R_RE_NO_AVAIL.match(line)
            used_with_avail = False

        if mr and (mode in ("", "-r")):
            ts = _parse_ts(mr.group("time"), mr.group("ampm"), self._current_date)

            metric = {
                "family": "sar",
                "name": "memory",
                "kb_mem_free": int(mr.group("kbfree")),
                "kb_mem_used": int(mr.group("kbused")),
                "mem_used_pct": _to_float(mr.group("memused_pct")),
            }

            if used_with_avail:
                try:
                    metric["kb_mem_avail"] = int(mr.group("kbavail"))  # type: ignore[arg-type]
                except Exception:
                    pass

            return ParsedEvent(
                timestamp_utc=ts,
                server=server,
                source=self.source,
                service="INFRA",
                message="sar -r memory",
                extra={"event_type": "metric", "metric": metric},
                log_upload_id=log_upload_id,
            )

        # ---- sar -d ----
        mdisk = D_RE.match(line)
        if mdisk and (mode in ("", "-d")):
            dev = mdisk.group("dev")
            if dev.upper() == "DEV":
                return None

            ts = _parse_ts(mdisk.group("time"), mdisk.group("ampm"), self._current_date)
            util = _to_float(mdisk.group("util"))
            tps = _to_float(mdisk.group("tps"))
            rest = (mdisk.group("rest") or "").strip()

            return ParsedEvent(
                timestamp_utc=ts,
                server=server,
                source=self.source,
                service="INFRA",
                message="sar -d disk",
                extra={
                    "event_type": "metric",
                    "metric": {
                        "family": "sar",
                        "name": "disk",
                        "device": dev,
                        "tps": tps,
                        "util_pct": util,
                        "rest": rest,
                    },
                },
                log_upload_id=log_upload_id,
            )

        return None
