# app/workers/parsing_worker_loop.py
from __future__ import annotations

import os
import time
import traceback
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models.event import Event
from app.models.log_upload import LogUpload
from app.models.system_setting import SystemSetting
from app.services.log_pipeline import parse_log_file


POLL_SECONDS_ENV = int(os.getenv("WORKER_POLL_SECONDS", "2").strip() or "2")
MAX_PER_CYCLE_ENV = int(os.getenv("WORKER_MAX_PER_CYCLE", "1").strip() or "1")

STALE_PARSING_SECONDS_ENV = int(os.getenv("WORKER_STALE_SECONDS", "3600").strip() or "3600")
MAX_PARSING_GLOBAL_ENV = int(os.getenv("WORKER_MAX_PROCESSING_GLOBAL", "9999").strip() or "9999")

# system_settings keys
KEY_ENABLED = "pipeline.parsing.enabled"
KEY_MAX_PER_CYCLE = "pipeline.parsing.max_per_cycle"
KEY_POLL_SECONDS = "pipeline.parsing.poll_seconds"
KEY_STALE_SECONDS = "pipeline.parsing.stale_seconds"
KEY_MAX_PROCESSING_GLOBAL = "pipeline.parsing.max_processing_global"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _utcnow().isoformat()


def _get_setting(db: Session, key: str) -> Optional[str]:
    row = db.query(SystemSetting).filter(SystemSetting.key == key).first()
    return row.value if row else None


def _get_bool(db: Session, key: str, default: bool) -> bool:
    raw = (_get_setting(db, key) or ("1" if default else "0")).strip().lower()
    return raw in ("1", "true", "yes", "y", "on", "enabled")


def _get_int(db: Session, key: str, default: int) -> int:
    raw = (_get_setting(db, key) or str(default)).strip()
    try:
        return int(raw)
    except Exception:
        return int(default)


def _to_utc(dt: Optional[datetime]) -> datetime:
    """
    Convierte datetime a UTC tz-aware.
    Si viene None, usa now UTC.
    """
    if not dt:
        return _utcnow()
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _utc_day_bounds(dt: datetime) -> Tuple[datetime, datetime]:
    """
    Retorna [start, end) del día UTC de dt.
    Esto ayuda a partition pruning cuando la tabla events está particionada por timestamp_utc (día).
    """
    dt_utc = _to_utc(dt)
    start = dt_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return start, end


def _requeue_stale_parsing(db: Session, stale_seconds: int) -> int:
    if stale_seconds <= 0:
        return 0

    now = _utcnow()
    changed = 0

    q = db.query(LogUpload).filter(LogUpload.status == "parsing")
    for job in q.limit(500).all():
        meta = job.extra_meta if isinstance(job.extra_meta, dict) else {}
        started = meta.get("parsing_started_at")
        if not started:
            continue
        try:
            dt = datetime.fromisoformat(str(started).replace("Z", "+00:00"))
            dt = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue

        age = (now - dt).total_seconds()
        if age >= stale_seconds:
            job.status = "queued"
            meta["worker_requeued_stale_at"] = _now_iso()
            meta["worker_requeued_reason"] = f"stale_parsing>{stale_seconds}s"
            job.extra_meta = meta
            changed += 1

    if changed:
        db.commit()

    return changed


def _count_parsing(db: Session) -> int:
    return int(db.query(LogUpload).filter(LogUpload.status == "parsing").count())


def _claim_one_job(db: Session) -> Optional[Tuple[int, str, str, str]]:
    row = db.execute(
        text(
            """
            SELECT id
            FROM log_uploads
            WHERE status = 'queued'
            ORDER BY uploaded_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            """
        )
    ).fetchone()

    if not row:
        return None

    upload_id = int(row[0])

    job = db.query(LogUpload).filter(LogUpload.id == upload_id).first()
    if not job:
        return None

    meta = job.extra_meta if isinstance(job.extra_meta, dict) else {}
    log_type = str(meta.get("log_type") or "").strip()

    job.status = "parsing"
    meta["worker_claimed_at"] = _now_iso()
    meta.setdefault("parsing_started_at", _now_iso())
    job.extra_meta = meta

    db.commit()
    db.refresh(job)

    return (job.id, job.path, job.server, log_type)


def _mark_error(db: Session, upload_id: int, err: str) -> None:
    job = db.query(LogUpload).filter(LogUpload.id == upload_id).first()
    if not job:
        return
    job.status = "error"
    job.error_message = (err or "")[:4000]
    meta = job.extra_meta if isinstance(job.extra_meta, dict) else {}
    meta["worker_failed_at"] = _now_iso()
    job.extra_meta = meta
    db.commit()


def _mark_parsed_or_processed(db: Session, upload_id: int) -> None:
    """
    Al terminar parse_log_file:
    - si el job sigue en 'parsing', lo marca como 'parsed'
    - guarda events_count
    - si events_count == 0 => marca 'processed' (porque engine no tiene nada que hacer)

    Optimización: acota por timestamp_utc (rango del día UTC del upload) para permitir partition pruning
    y mejor uso de índices, sin cambiar la lógica (seguimos contando exacto).
    """
    job = db.query(LogUpload).filter(LogUpload.id == upload_id).first()
    if not job:
        return

    # Preferimos uploaded_at para acotar al día; fallback seguro a now UTC si viniera vacío.
    uploaded_at = getattr(job, "uploaded_at", None)
    day_start, day_end = _utc_day_bounds(_to_utc(uploaded_at))

    events_count = int(
        db.query(Event.id)
        .filter(
            Event.log_upload_id == upload_id,
            Event.timestamp_utc >= day_start,
            Event.timestamp_utc < day_end,
        )
        .count()
    )

    meta = job.extra_meta if isinstance(job.extra_meta, dict) else {}
    meta["parsing_finished_at"] = _now_iso()
    meta["events_count"] = events_count
    job.extra_meta = meta

    if job.status == "parsing":
        job.status = "parsed"

    if events_count == 0 and job.status in ("parsed", "parsing"):
        job.status = "processed"
        meta["engine_finished_at"] = _now_iso()
        meta["note"] = "no_events_generated"
        job.extra_meta = meta

    db.commit()


def main() -> None:
    print(
        f"[parsing-worker] start poll={POLL_SECONDS_ENV}s max_per_cycle={MAX_PER_CYCLE_ENV} stale={STALE_PARSING_SECONDS_ENV}s",
        flush=True,
    )

    while True:
        db: Session = SessionLocal()
        try:
            enabled = _get_bool(db, KEY_ENABLED, default=True)
            poll_seconds = max(1, _get_int(db, KEY_POLL_SECONDS, default=POLL_SECONDS_ENV))
            max_per_cycle = max(1, _get_int(db, KEY_MAX_PER_CYCLE, default=MAX_PER_CYCLE_ENV))
            stale_seconds = max(0, _get_int(db, KEY_STALE_SECONDS, default=STALE_PARSING_SECONDS_ENV))
            max_processing_global = max(1, _get_int(db, KEY_MAX_PROCESSING_GLOBAL, default=MAX_PARSING_GLOBAL_ENV))

            if not enabled:
                time.sleep(poll_seconds)
                continue

            try:
                _requeue_stale_parsing(db, stale_seconds=stale_seconds)
            except Exception:
                db.rollback()

            try:
                if _count_parsing(db) >= max_processing_global:
                    time.sleep(poll_seconds)
                    continue
            except Exception:
                db.rollback()

            did_work = False

            for _ in range(max_per_cycle):
                claimed = _claim_one_job(db)
                if not claimed:
                    break

                did_work = True
                upload_id, file_path, server, log_type = claimed

                if not log_type:
                    _mark_error(db, upload_id, "Missing log_type in LogUpload.extra_meta")
                    continue

                try:
                    print(f"[parsing-worker] parsing id={upload_id} type={log_type} path={file_path}", flush=True)

                    parse_log_file(
                        file_path=file_path,
                        server=server,
                        log_type=log_type,
                        upload_id=upload_id,
                    )

                    # Si parse_log_file no actualiza status, lo hacemos aquí de forma segura
                    _mark_parsed_or_processed(db, upload_id)

                except Exception as e:
                    tb = traceback.format_exc()
                    _mark_error(db, upload_id, f"{type(e).__name__}: {e}\n{tb}")

        finally:
            db.close()

        if not did_work:
            time.sleep(poll_seconds)


if __name__ == "__main__":
    main()
