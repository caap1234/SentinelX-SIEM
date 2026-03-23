# app/services/exporter.py

from __future__ import annotations

import csv
import io
import json
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, BinaryIO

from sqlalchemy import text
from sqlalchemy.engine import Result
from sqlalchemy.orm import Session


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class ExportRequest:
    # Default seguro para evitar exports gigantes que generan temp masivo en PG
    days: int = 30

    # events, alerts, incidents, entities, api_keys, users, rules_v2, incident_rules
    # + NUEVO: events_excel, alerts_excel
    include: Optional[List[str]] = None

    # filtros “audit”
    alert_dispositions: Optional[List[str]] = None
    incident_dispositions: Optional[List[str]] = None
    statuses: Optional[List[str]] = None  # open/closed/false_positive etc (si aplica)

    # --- límites para evitar que Excel / ZIP fallen ---
    # Excel: 1,048,576 filas por hoja. CSV se suele abrir en Excel igual; dejamos margen.
    excel_split_rows: int = 1_000_000

    # Partir por tamaño para evitar entries gigantes dentro del ZIP.
    excel_split_bytes: int = 512 * 1024 * 1024  # 512MB


def _safe_list(v: Any) -> List[str]:
    if not v:
        return []
    return [str(x).strip() for x in v if str(x).strip()]


def _json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)


class _CountingBinaryWriter(io.RawIOBase):
    """
    Wrapper binario (file-like) que cuenta bytes escritos.
    Debe cumplir la interfaz que espera io.TextIOWrapper:
      - writable(), readable(), seekable()
      - write(), flush(), close()
    """
    def __init__(self, fp: BinaryIO):
        super().__init__()
        self._fp = fp
        self.bytes_written = 0

    def readable(self) -> bool:
        return False

    def writable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False

    def write(self, b: bytes) -> int:
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        n = self._fp.write(b)
        if n is None:
            n = len(b)
        self.bytes_written += n
        return n

    def flush(self) -> None:
        if self.closed:
            return
        self._fp.flush()

    def close(self) -> None:
        if self.closed:
            return
        try:
            self._fp.close()
        finally:
            super().close()


def build_export_zip_to_file(db: Session, req: ExportRequest, out_fp: BinaryIO) -> None:
    """
    Escribe el ZIP DIRECTO a un file-like (disco), evitando acumular todo en RAM.
    Ideal para backups grandes.

    Incluye:
      - exports raw: events.csv, alerts.csv, etc.
      - exports "excel-ready": events_excel.csv, alerts_excel.csv (flatten)
    """
    include = req.include or ["events", "alerts", "incidents", "entities"]
    include = [x.strip().lower() for x in include if x and str(x).strip()]

    since = _utc_now() - timedelta(days=max(1, int(req.days or 30)))

    # allowZip64=True soporta ZIPs grandes (y múltiples entries grandes)
    with zipfile.ZipFile(
        out_fp,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
        allowZip64=True,
    ) as z:
        meta = {
            "generated_at": _utc_now().isoformat(),
            "since": since.isoformat(),
            "include": include,
            "filters": {
                "alert_dispositions": _safe_list(req.alert_dispositions),
                "incident_dispositions": _safe_list(req.incident_dispositions),
                "statuses": _safe_list(req.statuses),
            },
            "excel_split": {
                "rows": int(req.excel_split_rows or 0),
                "bytes": int(req.excel_split_bytes or 0),
            },
        }
        z.writestr("meta.json", _json(meta).encode("utf-8"))

        # -----------------------------------------
        # RAW exports (compatibilidad / auditoría)
        # -----------------------------------------
        # IMPORTANTE: sin ORDER BY para evitar external sort/tmp masivo.
        if "events" in include:
            _write_query_csv_stream(
                db,
                z,
                filename="events.csv",
                sql="""
                    SELECT *
                    FROM events
                    WHERE timestamp_utc >= :since
                """,
                params={"since": since},
            )

        if "alerts" in include:
            where, params = _alert_where(since, req)
            _write_query_csv_stream(
                db,
                z,
                filename="alerts.csv",
                sql=f"""
                    SELECT *
                    FROM alerts
                    WHERE {where}
                """,
                params=params,
            )

        if "incidents" in include:
            where, params = _incident_where(since, req)
            _write_query_csv_stream(
                db,
                z,
                filename="incidents.csv",
                sql=f"""
                    SELECT *
                    FROM incidents
                    WHERE {where}
                """,
                params=params,
            )

        if "entities" in include:
            _write_query_csv_stream(
                db,
                z,
                filename="entities.csv",
                sql="""
                    SELECT *
                    FROM entities
                    WHERE updated_at >= :since
                """,
                params={"since": since},
            )

        if "rules_v2" in include:
            _write_query_csv_stream(
                db,
                z,
                filename="rules_v2.csv",
                sql="SELECT * FROM rules_v2",
                params={},
            )

        if "incident_rules" in include:
            _write_query_csv_stream(
                db,
                z,
                filename="incident_rules.csv",
                sql="SELECT * FROM incident_rules",
                params={},
            )

        if "api_keys" in include:
            _write_query_csv_stream(
                db,
                z,
                filename="api_keys.csv",
                sql="SELECT * FROM api_keys",
                params={},
            )

        if "users" in include:
            _write_query_csv_stream(
                db,
                z,
                filename="users.csv",
                sql="SELECT * FROM users",
                params={},
            )

        # -----------------------------------------
        # Excel-ready exports (flatten, estable)
        # -----------------------------------------
        # Estos crecen mucho: split por filas/tamaño para Excel y para evitar ZIP entry gigante.
        if "events_excel" in include:
            _write_query_csv_stream(
                db,
                z,
                filename="events_excel.csv",
                sql=_sql_events_excel(),
                params={"since": since},
                split_rows=int(req.excel_split_rows or 0) or None,
                split_bytes=int(req.excel_split_bytes or 0) or None,
            )

        if "alerts_excel" in include:
            where, params = _alert_where(since, req)
            _write_query_csv_stream(
                db,
                z,
                filename="alerts_excel.csv",
                sql=_sql_alerts_excel(where),
                params=params,
                split_rows=int(req.excel_split_rows or 0) or None,
                split_bytes=int(req.excel_split_bytes or 0) or None,
            )


def build_export_zip(db: Session, req: ExportRequest) -> bytes:
    """
    Backward compatible: genera bytes (RAM). Útil para exports pequeños.
    Para backups grandes usa build_export_zip_to_file().
    """
    buf = io.BytesIO()
    build_export_zip_to_file(db, req, buf)
    return buf.getvalue()


def _split_name(filename: str, part_no: int) -> str:
    """
    events_excel.csv -> events_excel_part001.csv
    """
    if part_no <= 1:
        return filename
    if "." in filename:
        base, ext = filename.rsplit(".", 1)
        return f"{base}_part{part_no:03d}.{ext}"
    return f"{filename}_part{part_no:03d}"


def _write_query_csv_stream(
    db: Session,
    z: zipfile.ZipFile,
    *,
    filename: str,
    sql: str,
    params: Dict[str, Any],
    fetch_size: int = 5000,
    split_rows: Optional[int] = None,
    split_bytes: Optional[int] = None,
) -> None:
    """
    Escribe un CSV dentro del ZIP en modo streaming (sin fetchall()).

    - stream_results=True pide a SQLAlchemy/driver no cargar todo en memoria.
    - fetchmany() baja consumo de RAM.
    - Importante: aunque streamée, si el SQL requiere SORT/HASH grande,
      Postgres puede escribir temp files.

    ZIP64:
    - Para CSVs enormes, zipfile necesita saber desde el inicio que la entrada usará ZIP64,
      o al cerrar puede fallar con: "File size unexpectedly exceeded ZIP64 limit".
    - Solución: z.open(..., force_zip64=True)

    SPLIT:
    - split_rows: parte el CSV en múltiples entries para que Excel/imports no fallen.
    - split_bytes: parte por tamaño para evitar entries gigantes.
    """
    stmt = text(sql)
    res: Result = db.execute(stmt.execution_options(stream_results=True), params)

    # Handles del ZIP (deben cerrarse SIEMPRE antes de que cierre ZipFile)
    zf: Optional[BinaryIO] = None
    wrapper: Optional[io.TextIOWrapper] = None
    writer: Optional[csv.writer] = None
    counter: Optional[_CountingBinaryWriter] = None

    part_no = 1
    rows_in_part = 0  # filas de datos (sin header)

    def close_part() -> None:
        nonlocal zf, wrapper, writer, counter
        # Cerrar WRAPPER es lo importante: eso cierra counter -> cierra zf
        if wrapper is not None:
            try:
                wrapper.close()
            except Exception:
                pass
        wrapper = None
        writer = None
        counter = None
        zf = None

    def open_part(pn: int, cols: List[str]) -> None:
        nonlocal zf, wrapper, writer, counter, rows_in_part
        close_part()

        rows_in_part = 0
        part_name = _split_name(filename, pn)

        # force_zip64=True evita RuntimeError si el CSV interno crece mucho
        zf = z.open(part_name, mode="w", force_zip64=True)

        # Contador binario compatible con TextIOWrapper
        counter = _CountingBinaryWriter(zf)
        wrapper = io.TextIOWrapper(counter, encoding="utf-8-sig", newline="")
        writer = csv.writer(wrapper)
        writer.writerow(cols)

    def should_split_next_row() -> bool:
        # Sin split configurado: no partimos
        if not split_rows and not split_bytes:
            return False
        # Partir por filas
        if split_rows and rows_in_part >= split_rows:
            return True
        # Partir por bytes (incluye header/BOM)
        if split_bytes and counter is not None and counter.bytes_written >= split_bytes:
            return True
        return False

    try:
        cols = list(res.keys())
        open_part(part_no, cols)

        while True:
            batch = res.fetchmany(fetch_size)
            if not batch:
                break

            for row in batch:
                if should_split_next_row():
                    part_no += 1
                    open_part(part_no, cols)

                assert writer is not None
                writer.writerow(list(row))
                rows_in_part += 1

        close_part()

    finally:
        # Asegura que no quede handle de escritura abierto
        try:
            close_part()
        except Exception:
            pass

        try:
            res.close()
        except Exception:
            pass


def _alert_where(since: datetime, req: ExportRequest) -> Tuple[str, Dict[str, Any]]:
    where = ["triggered_at >= :since"]
    params: Dict[str, Any] = {"since": since}

    disp = _safe_list(req.alert_dispositions)
    if disp:
        where.append("COALESCE(disposition,'') = ANY(:disp)")
        params["disp"] = disp

    st = _safe_list(req.statuses)
    if st:
        where.append("COALESCE(status,'') = ANY(:st)")
        params["st"] = st

    return " AND ".join(where), params


def _incident_where(since: datetime, req: ExportRequest) -> Tuple[str, Dict[str, Any]]:
    where = ["last_activity_at >= :since"]
    params: Dict[str, Any] = {"since": since}

    disp = _safe_list(req.incident_dispositions)
    if disp:
        where.append("COALESCE(disposition,'') = ANY(:disp)")
        params["disp"] = disp

    st = _safe_list(req.statuses)
    if st:
        where.append("COALESCE(status,'') = ANY(:st)")
        params["st"] = st

    return " AND ".join(where), params


# --------------------------------------------------------------------
# SQL builders: Excel-ready flatten
# --------------------------------------------------------------------

def _sql_events_excel() -> str:
    """
    Export estable para Excel:
    - 1 fila por evento
    - extrae campos de e.extra (http/geo/asn/metric/mail/lfd/auth)
    - deriva date/month/week_iso/hour
    - calcula subnet24 desde ip_client (si existe)

    NOTA: Evitamos ORDER BY.
    """
    return """
        SELECT
            e.id::text                                              AS event_id,
            (e.timestamp_utc AT TIME ZONE 'UTC')                    AS timestamp_utc,
            to_char(e.timestamp_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS date,
            to_char(e.timestamp_utc AT TIME ZONE 'UTC', 'YYYY-MM')    AS month,
            to_char(e.timestamp_utc AT TIME ZONE 'UTC', 'IYYY-IW')    AS week_iso,
            EXTRACT(HOUR FROM (e.timestamp_utc AT TIME ZONE 'UTC'))::int AS hour,

            e.server                                                AS server,
            e.source                                                AS source,
            e.service                                               AS service,
            COALESCE(NULLIF(e.domain,''), NULLIF(e.extra->>'vhost','')) AS domain,
            e.username                                              AS username,
            e.ip_client::text                                       AS ip_client,

            CASE
                WHEN e.ip_client IS NULL THEN NULL
                ELSE set_masklen(e.ip_client, 24)::text
            END                                                     AS ip_subnet24,

            -- enrichment (observado en tu extra)
            COALESCE(e.extra->'geo'->>'country_code', e.extra->'geoip'->>'country_code') AS country_code,
            (e.extra->'asn'->>'number')                              AS asn_number,
            (e.extra->'asn'->>'org')                                 AS asn_org,

            -- clasificación principal
            (e.extra->>'event_type')                                 AS event_type,
            (e.extra->>'action')                                     AS action,
            (e.extra->>'protocol')                                   AS protocol,
            (e.extra->>'kind')                                       AS kind,
            (e.extra->>'reason')                                     AS reason,
            (e.extra->>'direction')                                  AS direction,

            -- HTTP
            (e.extra->'http'->>'method')                              AS http_method,
            (e.extra->'http'->>'path')                                AS http_path,
            split_part((e.extra->'http'->>'path'), '?', 1)            AS http_path_clean,
            (e.extra->'http'->>'status')::int                         AS http_status,
            CASE
                WHEN (e.extra->'http'->>'status') IS NULL THEN NULL
                ELSE (( (e.extra->'http'->>'status')::int / 100) * 100)::text || 'xx'
            END                                                      AS http_status_class,
            (e.extra->'http'->>'bytes')                               AS http_bytes,
            (e.extra->'http'->>'referer')                             AS http_referer,
            COALESCE(e.extra->'http'->>'referer_host', e.extra->>'referer_host') AS referer_host,
            (e.extra->'http'->>'host')                                AS http_host,
            (e.extra->'http'->>'user_agent')                          AS user_agent,

            -- categorización rápida (puedes refinar luego)
            CASE
                WHEN (e.extra->'http'->>'path') ILIKE '%/wp-login.php%' THEN 'wp-login'
                WHEN (e.extra->'http'->>'path') ILIKE '%/xmlrpc.php%' THEN 'xmlrpc'
                WHEN (e.extra->'http'->>'path') ILIKE '%/.env%' THEN 'sensitive'
                WHEN (e.extra->'http'->>'path') ILIKE '%/wp-config.php%' THEN 'sensitive'
                WHEN (e.extra->'http'->>'path') ILIKE '%/.git%' THEN 'sensitive'
                WHEN (e.extra->'http'->>'path') ILIKE '%/cgi-sys/%' THEN 'cpanel-cgi'
                ELSE NULL
            END                                                      AS path_category,

            CASE
                WHEN (e.extra->'http'->>'path') IS NULL THEN 0
                WHEN (e.extra->'http'->>'path') ILIKE '%/.env%' THEN 1
                WHEN (e.extra->'http'->>'path') ILIKE '%/wp-config.php%' THEN 1
                WHEN (e.extra->'http'->>'path') ILIKE '%/.git%' THEN 1
                WHEN (e.extra->'http'->>'path') ILIKE '%/backup%' THEN 1
                WHEN (e.extra->'http'->>'path') ILIKE '%/dump%' THEN 1
                ELSE 0
            END                                                      AS is_sensitive_path,

            -- AUTH (si aplica)
            (e.extra->>'auth_user')                                   AS auth_user,
            (e.extra->>'auth_method')                                 AS auth_method,
            (e.extra->>'auth_success')                                AS auth_success,

            -- PANEL / WAF / FILE (si aplica)
            (e.extra->'panel'->>'area')                               AS panel_area,
            (e.extra->'panel'->>'action')                             AS panel_action,
            (e.extra->'panel'->>'status')                             AS panel_status,
            (e.extra->'file'->>'action')                              AS file_action,
            (e.extra->'file'->>'dir')                                 AS file_dir,

            (e.extra->'waf'->>'rule_id')                              AS waf_rule_id,

            -- MAIL (si aplica)
            (e.extra->>'from')                                        AS mail_from,
            (e.extra->>'to')                                          AS mail_to,
            (e.extra->>'subject')                                     AS mail_subject,
            (e.extra->>'recipient')                                   AS mail_recipient,

            -- METRICS (SAR)
            (e.extra->'metric'->>'family')                             AS metric_family,
            (e.extra->'metric'->>'name')                               AS metric_name,
            (e.extra->'metric'->>'ldavg_1')                            AS ldavg_1,
            (e.extra->'metric'->>'ldavg_5')                            AS ldavg_5,
            (e.extra->'metric'->>'ldavg_1_per_cpu')                    AS ldavg_1_per_cpu,
            (e.extra->'metric'->>'runq_sz')                            AS runq_sz,
            (e.extra->'metric'->>'blocked')                            AS blocked,
            (e.extra->'metric'->>'mem_used_pct')                       AS mem_used_pct,

            -- message y extra completo (útil para debugging)
            e.message                                                 AS message,
            e.extra::text                                             AS extra_json

        FROM events e
        WHERE e.timestamp_utc >= :since
    """


def _sql_alerts_excel(where_clause: str) -> str:
    """
    Export estable para Excel:
    - 1 fila por alerta
    - rule_code y family salen del rule_name (por tu formato "AUTH-001 | ...")
    - entidades salen de evidence.group_values (como en tu sample)
    - métricas vienen de alerts.metrics
    """
    return f"""
        SELECT
            a.id                                                     AS alert_id,
            (a.triggered_at AT TIME ZONE 'UTC')                      AS triggered_at_utc,
            (a.window_start AT TIME ZONE 'UTC')                      AS window_start_utc,
            (a.window_end   AT TIME ZONE 'UTC')                      AS window_end_utc,

            a.status                                                 AS status,
            a.disposition                                            AS disposition,
            a.severity                                               AS severity,

            a.rule_id                                                AS rule_id,
            split_part(a.rule_name, ' | ', 1)                         AS rule_code,
            split_part(split_part(a.rule_name, ' | ', 1), '-', 1)      AS family,
            a.rule_name                                              AS rule_name,

            a.server                                                 AS server,
            a.source                                                 AS source,
            a.event_type                                             AS event_type,
            a.group_key                                              AS group_key,

            -- entidades (desde evidence.group_values; ya lo tienes)
            (a.evidence->'group_values'->>'server')                   AS entity_server,
            (a.evidence->'group_values'->>'username')                 AS entity_username,
            (a.evidence->'group_values'->>'ip_client')                AS entity_ip_client,
            (a.evidence->'group_values'->>'extra.vhost')              AS entity_vhost,
            (a.evidence->'group_values'->>'extra.geo.country_code')   AS entity_country_code,
            (a.evidence->'group_values'->>'extra.asn.number')         AS entity_asn_number,
            (a.evidence->'group_values'->>'extra.asn.org')            AS entity_asn_org,

            -- subnet24 calculable desde entity_ip_client si aplica (guardamos texto, Excel lo usa)
            CASE
                WHEN (a.evidence->'group_values'->>'ip_client') IS NULL THEN NULL
                ELSE set_masklen((a.evidence->'group_values'->>'ip_client')::inet, 24)::text
            END                                                      AS entity_subnet24,

            -- métricas (tu metrics ya trae estas keys)
            (a.metrics->>'count')::int                                AS count_in_window,
            (a.metrics->>'fail_count')::int                           AS fail_count,
            (a.metrics->>'success_count')::int                        AS success_count,

            (a.metrics->>'unique_ips')::int                           AS unique_ips,
            (a.metrics->>'unique_users')::int                         AS unique_users,
            (a.metrics->>'unique_paths')::int                         AS unique_paths,
            (a.metrics->>'unique_servers')::int                       AS unique_servers,
            (a.metrics->>'unique_subnets24')::int                      AS unique_subnets24,

            (a.metrics->>'top_subnet24')                               AS top_subnet24,
            (a.metrics->>'top_subnet24_hits')::int                     AS top_subnet24_hits,
            (a.metrics->>'window_seconds')::int                        AS window_seconds,
            (a.metrics->>'trusted')::boolean                           AS trusted,

            -- evidence útil para drill-down
            a.evidence::text                                          AS evidence_json,
            a.metrics::text                                           AS metrics_json,

            a.created_at AT TIME ZONE 'UTC'                           AS created_at_utc,
            a.resolved_at AT TIME ZONE 'UTC'                          AS resolved_at_utc,
            a.resolved_by                                             AS resolved_by,
            a.resolution_note                                         AS resolution_note

        FROM alerts a
        WHERE {where_clause}
    """
