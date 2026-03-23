#app/workers/engine_worker_loop.py
from __future__ import annotations

import os
import socket
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models.event import Event
from app.models.log_upload import LogUpload
from app.models.system_setting import SystemSetting
from app.services.rule_engine_runtime import get_rule_engine, invalidate_rule_engine_cache

try:
    from sqlalchemy.orm.exc import DetachedInstanceError
except Exception:  # pragma: no cover
    DetachedInstanceError = Exception  # type: ignore


# -------------------------
# Env defaults
# -------------------------
POLL_SECONDS_ENV = int(os.getenv("ENGINE_WORKER_POLL_SECONDS", "1").strip() or "1")
MAX_PER_CYCLE_ENV = int(os.getenv("ENGINE_WORKER_MAX_PER_CYCLE", "200").strip() or "200")
STALE_PROCESSING_SECONDS_ENV = int(os.getenv("ENGINE_WORKER_STALE_SECONDS", "3600").strip() or "3600")
RECONCILE_EVERY_CYCLES_ENV = int(os.getenv("ENGINE_WORKER_RECONCILE_EVERY", "10").strip() or "10")
LOOKBACK_HOURS_ENV = int(os.getenv("ENGINE_WORKER_LOOKBACK_HOURS", "48").strip() or "48")

# Claim en micro-batches
CLAIM_BATCH_SIZE_ENV = int(os.getenv("ENGINE_WORKER_CLAIM_BATCH_SIZE", "50").strip() or "50")

# Anti-freeze DB (evita waits infinitos por locks/queries dentro de engine.on_event)
DB_LOCK_TIMEOUT_MS_ENV = int(os.getenv("ENGINE_WORKER_DB_LOCK_TIMEOUT_MS", "3000").strip() or "3000")  # 3s
DB_STATEMENT_TIMEOUT_MS_ENV = int(os.getenv("ENGINE_WORKER_DB_STATEMENT_TIMEOUT_MS", "60000").strip() or "60000")  # 60s

# Anti-freeze SMTP (NO toca emailer): timeout de sockets durante notify
SMTP_NOTIFY_TIMEOUT_SECONDS_ENV = float(os.getenv("ENGINE_WORKER_SMTP_TIMEOUT", "12").strip() or "12")

# “Que se tomen un momento” entre emails (por worker)
NOTIFY_SLEEP_MS_ENV = int(os.getenv("ENGINE_WORKER_NOTIFY_SLEEP_MS", "200").strip() or "200")

# Retries notify (por email)
NOTIFY_MAX_RETRIES_ENV = int(os.getenv("ENGINE_WORKER_NOTIFY_MAX_RETRIES", "2").strip() or "2")
NOTIFY_RETRY_BASE_MS_ENV = int(os.getenv("ENGINE_WORKER_NOTIFY_RETRY_BASE_MS", "300").strip() or "300")

# Límite de notificaciones por ciclo (por worker)
NOTIFY_MAX_PER_CYCLE_ENV = int(os.getenv("ENGINE_WORKER_NOTIFY_MAX_PER_CYCLE", "500").strip() or "500")

# Rescue más agresivo (por si se quedan en processing sin avanzar)
# Si un evento quedó processing más de RESCUE_PROCESSING_SECONDS, lo reencolamos aunque stale_seconds sea grande.
RESCUE_PROCESSING_SECONDS_ENV = int(os.getenv("ENGINE_WORKER_RESCUE_PROCESSING_SECONDS", "300").strip() or "300")  # 5 min
RESCUE_LIMIT_ENV = int(os.getenv("ENGINE_WORKER_RESCUE_LIMIT", "2000").strip() or "2000")


# -------------------------
# system_settings keys
# -------------------------
KEY_ENABLED = "pipeline.engine.enabled"
KEY_MAX_PER_CYCLE = "pipeline.engine.max_per_cycle"
KEY_POLL_SECONDS = "pipeline.engine.poll_seconds"
KEY_STALE_SECONDS = "pipeline.engine.stale_seconds"
KEY_LOOKBACK_HOURS = "pipeline.engine.lookback_hours"
KEY_CLAIM_BATCH_SIZE = "pipeline.engine.claim_batch_size"


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


def _since_utc(lookback_hours: int) -> Optional[datetime]:
    if lookback_hours <= 0:
        return None
    return _utcnow() - timedelta(hours=int(lookback_hours))


@dataclass
class ClaimedEvent:
    id: str  # UUID string
    timestamp_utc: datetime
    log_upload_id: Optional[int]


def _set_tx_timeouts(db: Session, *, lock_timeout_ms: int, statement_timeout_ms: int) -> None:
    """
    Aplica límites por transacción para evitar bloqueos infinitos.
    - SET LOCAL solo vive dentro de la transacción actual.
    """
    # lock_timeout / statement_timeout esperan strings tipo '3000ms'
    db.execute(text("SET LOCAL lock_timeout = :v"), {"v": f"{int(lock_timeout_ms)}ms"})
    db.execute(text("SET LOCAL statement_timeout = :v"), {"v": f"{int(statement_timeout_ms)}ms"})


def _requeue_processing_older_than(
    db: Session,
    *,
    older_than_seconds: int,
    limit: int,
    reason: str,
) -> int:
    """
    Re-encola eventos processing más viejos que X.
    Crucial para particiones: usamos (timestamp_utc + id) para rutear update.
    No dependemos de lookback por timestamp_utc (porque puede venir viejo).
    """
    if older_than_seconds <= 0:
        return 0

    cutoff = _utcnow() - timedelta(seconds=int(older_than_seconds))
    rows = db.execute(
        text(
            """
            WITH cte AS (
              SELECT e.timestamp_utc, e.id
              FROM events e
              WHERE e.engine_status = 'processing'
                AND e.engine_claimed_at IS NOT NULL
                AND e.engine_claimed_at < :cutoff
              ORDER BY e.engine_claimed_at ASC
              LIMIT :limit
              FOR UPDATE SKIP LOCKED
            )
            UPDATE events e2
            SET engine_status = 'pending',
                engine_claimed_at = NULL,
                engine_error = :msg
            FROM cte
            WHERE e2.timestamp_utc = cte.timestamp_utc
              AND e2.id = cte.id
            RETURNING e2.id
            """
        ),
        {"cutoff": cutoff, "limit": int(limit), "msg": f"{reason} at {_now_iso()}"},
    ).fetchall()
    return len(rows or [])


def _claim_batch(db: Session, lookback_hours: int, limit: int) -> List[ClaimedEvent]:
    """
    Claim robusto:
      - lookback por created_at (entrada al sistema), porque timestamp_utc puede venir atrasado
      - devolvemos timestamp_utc para rutear UPDATE a la partición correcta
    """
    since_created = _since_utc(lookback_hours)

    created_filter = ""
    params = {"now": _utcnow(), "limit": int(limit)}
    if since_created is not None:
        created_filter = "AND e.created_at >= :since_created"
        params["since_created"] = since_created

    rows = db.execute(
        text(
            f"""
            WITH cte AS (
              SELECT e.timestamp_utc, e.id
              FROM events e
              WHERE e.engine_status = 'pending'
                {created_filter}
              ORDER BY e.created_at ASC
              LIMIT :limit
              FOR UPDATE SKIP LOCKED
            )
            UPDATE events e2
            SET engine_status = 'processing',
                engine_claimed_at = :now,
                engine_attempts = COALESCE(engine_attempts, 0) + 1,
                engine_error = NULL
            FROM cte
            WHERE e2.timestamp_utc = cte.timestamp_utc
              AND e2.id = cte.id
            RETURNING e2.id, e2.timestamp_utc, e2.log_upload_id
            """
        ),
        params,
    ).fetchall()

    out: List[ClaimedEvent] = []
    for r in rows or []:
        out.append(ClaimedEvent(id=str(r[0]), timestamp_utc=r[1], log_upload_id=r[2]))
    return out


def _maybe_mark_upload_processed(db: Session, upload_id: int, lookback_hours: int) -> None:
    if not upload_id:
        return

    since = _since_utc(lookback_hours)

    q = db.query(Event.id).filter(
        Event.log_upload_id == upload_id,
        Event.engine_status.in_(["pending", "processing"]),
    )
    if since is not None:
        q = q.filter(Event.timestamp_utc >= since)

    still = q.limit(1).first()
    if still:
        return

    log = db.query(LogUpload).filter(LogUpload.id == upload_id).first()
    if not log:
        return

    if log.status in ("parsed", "processing_engine", "parsing"):
        log.status = "processed"
        meta = log.extra_meta if isinstance(log.extra_meta, dict) else {}
        meta["engine_finished_at"] = _now_iso()
        log.extra_meta = meta


def _reconcile_uploads_processing_engine(db: Session, limit: int = 500, lookback_hours: int = 48) -> int:
    """
    Evita uploads pegados.
    Importante: para no depender de timestamp_utc, el filtro temporal lo hacemos por events.created_at.
    """
    since_created = _since_utc(lookback_hours)
    created_filter = ""
    params = {"limit": int(limit), "now": _now_iso()}
    if since_created is not None:
        created_filter = "AND e.created_at >= :since_created"
        params["since_created"] = since_created

    rows = db.execute(
        text(
            f"""
            WITH cte AS (
              SELECT lu.id
              FROM log_uploads lu
              WHERE lu.status = 'processing_engine'
                AND NOT EXISTS (
                  SELECT 1
                  FROM events e
                  WHERE e.log_upload_id = lu.id
                    AND e.engine_status IN ('pending','processing')
                    {created_filter}
                )
              ORDER BY lu.uploaded_at DESC
              LIMIT :limit
              FOR UPDATE SKIP LOCKED
            )
            UPDATE log_uploads lu
            SET status = 'processed',
                extra_meta = COALESCE(lu.extra_meta, '{{}}'::jsonb) ||
                           jsonb_build_object('engine_finished_at', :now)
            FROM cte
            WHERE lu.id = cte.id
            RETURNING lu.id
            """
        ),
        params,
    ).fetchall()

    return len(rows or [])


def _mark_event_error(db: Session, event_id: str, ts: datetime, err: str) -> None:
    db.execute(
        text(
            """
            UPDATE events
            SET engine_status = 'error',
                engine_processed_at = :now,
                engine_error = :err
            WHERE timestamp_utc = :ts AND id = :id
            """
        ),
        {"now": _utcnow(), "err": (err or "")[:8000], "ts": ts, "id": event_id},
    )


def _mark_event_done(db: Session, event_id: str, ts: datetime) -> None:
    db.execute(
        text(
            """
            UPDATE events
            SET engine_status = 'done',
                engine_processed_at = :now,
                engine_error = NULL
            WHERE timestamp_utc = :ts AND id = :id
            """
        ),
        {"now": _utcnow(), "ts": ts, "id": event_id},
    )


def _resolve_user_id_for_notify(db: Session, log: Optional[LogUpload]) -> Optional[int]:
    if log is None:
        return None

    uid = getattr(log, "user_id", None)

    if uid is None:
        api_key_id = getattr(log, "api_key_id", None)
        if api_key_id:
            row = db.execute(
                text("SELECT created_by_user_id FROM api_keys WHERE id = :id"),
                {"id": int(api_key_id)},
            ).fetchone()
            if row:
                uid = row[0]

    try:
        return int(uid) if uid is not None else None
    except Exception:
        return None


def _notify_alerts_post_commit(
    *,
    alert_ids: List,
    user_id: int,
    smtp_timeout_seconds: float,
    sleep_ms: int,
    max_retries: int,
    retry_base_ms: int,
    max_per_cycle: int,
) -> None:
    """
    Notifica alertas FUERA de la transacción del engine.
    NO toca emailer; solo:
      - timeout duro por socket para que NO se congele
      - retries con backoff
      - sleep entre correos (tu “tómense un momento”)
    """
    if not alert_ids or not user_id:
        return

    try:
        from app.models.alert import Alert
    except Exception as e:
        print(f"[engine-worker] Alert model import failed: {type(e).__name__}: {e}", flush=True)
        return

    try:
        from app.services.notification_dispatch import notify_on_alert_created, smtp_configured  # type: ignore
    except Exception as e:
        tb = traceback.format_exc()
        print(
            f"[engine-worker] notifier import failed (notification_dispatch): {type(e).__name__}: {e}\n{tb}",
            flush=True,
        )
        return

    if not smtp_configured():
        print("[engine-worker] SMTP no configurado (SMTP_HOST/SMTP_PORT/FROM_EMAIL). Notificaciones omitidas.", flush=True)
        return

    prev_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(float(smtp_timeout_seconds))

    dbn: Session = SessionLocal()
    sent = 0
    try:
        for aid in alert_ids:
            if not aid:
                continue
            if sent >= int(max_per_cycle):
                print(f"[engine-worker] notify capped sent={sent} cap={max_per_cycle}", flush=True)
                break

            al = dbn.query(Alert).filter(Alert.id == aid).first()
            if not al:
                continue

            # retries
            last_exc: Optional[BaseException] = None
            for attempt in range(0, max(0, int(max_retries)) + 1):
                try:
                    notify_on_alert_created(db=dbn, alert=al, user_id=int(user_id))
                    last_exc = None
                    break
                except Exception as e:
                    last_exc = e
                    backoff_ms = int(retry_base_ms) * (2**attempt)
                    backoff_ms = min(backoff_ms, 5000)
                    print(
                        f"[engine-worker] notify failed attempt={attempt} alert_id={aid} user_id={user_id}: "
                        f"{type(e).__name__}: {e} (sleep {backoff_ms}ms)",
                        flush=True,
                    )
                    time.sleep(max(0.0, backoff_ms / 1000.0))

            if last_exc is None:
                sent += 1

            if sleep_ms > 0:
                time.sleep(max(0.0, int(sleep_ms) / 1000.0))

    finally:
        dbn.close()
        socket.setdefaulttimeout(prev_timeout)


def main() -> None:
    print(
        f"[engine-worker] start poll={POLL_SECONDS_ENV}s "
        f"max_per_cycle={MAX_PER_CYCLE_ENV} stale={STALE_PROCESSING_SECONDS_ENV}s "
        f"lookback_h={LOOKBACK_HOURS_ENV} claim_batch={CLAIM_BATCH_SIZE_ENV}",
        flush=True,
    )

    engine = get_rule_engine()
    cycle = 0

    while True:
        poll_seconds = POLL_SECONDS_ENV
        max_per_cycle = MAX_PER_CYCLE_ENV
        stale_seconds = STALE_PROCESSING_SECONDS_ENV
        lookback_hours = LOOKBACK_HOURS_ENV
        claim_batch_size = CLAIM_BATCH_SIZE_ENV

        lock_timeout_ms = DB_LOCK_TIMEOUT_MS_ENV
        statement_timeout_ms = DB_STATEMENT_TIMEOUT_MS_ENV

        smtp_timeout_seconds = SMTP_NOTIFY_TIMEOUT_SECONDS_ENV
        notify_sleep_ms = NOTIFY_SLEEP_MS_ENV
        notify_max_retries = NOTIFY_MAX_RETRIES_ENV
        notify_retry_base_ms = NOTIFY_RETRY_BASE_MS_ENV
        notify_max_per_cycle = NOTIFY_MAX_PER_CYCLE_ENV

        rescue_seconds = RESCUE_PROCESSING_SECONDS_ENV
        rescue_limit = RESCUE_LIMIT_ENV

        did_work = False

        db: Session = SessionLocal()
        try:
            enabled = _get_bool(db, KEY_ENABLED, default=True)
            poll_seconds = max(1, _get_int(db, KEY_POLL_SECONDS, default=POLL_SECONDS_ENV))
            max_per_cycle = max(1, _get_int(db, KEY_MAX_PER_CYCLE, default=MAX_PER_CYCLE_ENV))
            stale_seconds = max(0, _get_int(db, KEY_STALE_SECONDS, default=STALE_PROCESSING_SECONDS_ENV))
            lookback_hours = max(0, _get_int(db, KEY_LOOKBACK_HOURS, default=LOOKBACK_HOURS_ENV))
            claim_batch_size = max(1, _get_int(db, KEY_CLAIM_BATCH_SIZE, default=CLAIM_BATCH_SIZE_ENV))

            if not enabled:
                time.sleep(poll_seconds)
                continue

            # 1) Rescue rápido: reencola processing viejos (por si se colgaron workers)
            try:
                with db.begin():
                    _set_tx_timeouts(db, lock_timeout_ms=lock_timeout_ms, statement_timeout_ms=statement_timeout_ms)
                    rescued = _requeue_processing_older_than(
                        db,
                        older_than_seconds=rescue_seconds,
                        limit=rescue_limit,
                        reason=f"rescue_processing>{rescue_seconds}s",
                    )
                if rescued:
                    print(f"[engine-worker] rescued processing requeued={rescued}", flush=True)
            except Exception:
                db.rollback()

            # 2) Stale normal (más conservador)
            try:
                with db.begin():
                    _set_tx_timeouts(db, lock_timeout_ms=lock_timeout_ms, statement_timeout_ms=statement_timeout_ms)
                    n = _requeue_processing_older_than(
                        db,
                        older_than_seconds=stale_seconds,
                        limit=2000,
                        reason=f"stale_processing>{stale_seconds}s",
                    )
                if n:
                    print(f"[engine-worker] stale requeued={n}", flush=True)
            except Exception:
                db.rollback()

            # 3) Reconciliación periódica
            cycle += 1
            if RECONCILE_EVERY_CYCLES_ENV > 0 and (cycle % RECONCILE_EVERY_CYCLES_ENV) == 0:
                try:
                    with db.begin():
                        _set_tx_timeouts(db, lock_timeout_ms=lock_timeout_ms, statement_timeout_ms=statement_timeout_ms)
                        fixed = _reconcile_uploads_processing_engine(db, limit=500, lookback_hours=lookback_hours)
                    if fixed:
                        print(f"[engine-worker] reconciled uploads fixed={fixed}", flush=True)
                except Exception:
                    db.rollback()

            processed_this_cycle = 0
            local_queue: List[ClaimedEvent] = []

            while processed_this_cycle < max_per_cycle:
                if not local_queue:
                    try:
                        with db.begin():
                            _set_tx_timeouts(db, lock_timeout_ms=lock_timeout_ms, statement_timeout_ms=statement_timeout_ms)
                            local_queue = _claim_batch(
                                db,
                                lookback_hours=lookback_hours,
                                limit=min(claim_batch_size, max_per_cycle - processed_this_cycle),
                            )
                    except Exception:
                        db.rollback()
                        local_queue = []

                    if not local_queue:
                        break

                ce = local_queue.pop(0)
                did_work = True

                alert_ids_to_notify: List = []
                notify_user_id: Optional[int] = None

                try:
                    with db.begin():
                        _set_tx_timeouts(db, lock_timeout_ms=lock_timeout_ms, statement_timeout_ms=statement_timeout_ms)

                        ev = (
                            db.query(Event)
                            .filter(Event.timestamp_utc == ce.timestamp_utc, Event.id == ce.id)
                            .first()
                        )
                        if not ev:
                            processed_this_cycle += 1
                            continue

                        log: Optional[LogUpload] = None
                        if ce.log_upload_id:
                            log = db.query(LogUpload).filter(LogUpload.id == int(ce.log_upload_id)).first()
                            if log and log.status in ("parsed", "parsing"):
                                log.status = "processing_engine"
                                meta = log.extra_meta if isinstance(log.extra_meta, dict) else {}
                                meta.setdefault("engine_started_at", _now_iso())
                                log.extra_meta = meta

                        try:
                            created_alerts = engine.on_event(db, ev)
                        except DetachedInstanceError as e:
                            print(f"[engine-worker] DetachedInstanceError -> hard reload + retry: {e}", flush=True)
                            invalidate_rule_engine_cache(hard=True)
                            engine = get_rule_engine()
                            created_alerts = engine.on_event(db, ev)

                        db.flush()

                        notify_user_id = _resolve_user_id_for_notify(db, log)

                        for a in (created_alerts or []):
                            aid = getattr(a, "id", None)
                            if aid is not None:
                                alert_ids_to_notify.append(aid)

                        _mark_event_done(db, ce.id, ce.timestamp_utc)

                        if ce.log_upload_id:
                            _maybe_mark_upload_processed(db, int(ce.log_upload_id), lookback_hours=lookback_hours)

                    # FUERA de la tx: notifica (anti-freeze + “momentito”)
                    if alert_ids_to_notify and notify_user_id:
                        _notify_alerts_post_commit(
                            alert_ids=alert_ids_to_notify,
                            user_id=int(notify_user_id),
                            smtp_timeout_seconds=float(smtp_timeout_seconds),
                            sleep_ms=int(notify_sleep_ms),
                            max_retries=int(notify_max_retries),
                            retry_base_ms=int(notify_retry_base_ms),
                            max_per_cycle=int(notify_max_per_cycle),
                        )

                except Exception as e:
                    tb = traceback.format_exc()
                    err = f"{type(e).__name__}: {e}\n{tb}"
                    try:
                        with db.begin():
                            _set_tx_timeouts(db, lock_timeout_ms=lock_timeout_ms, statement_timeout_ms=statement_timeout_ms)
                            _mark_event_error(db, ce.id, ce.timestamp_utc, err)
                    except Exception:
                        db.rollback()

                processed_this_cycle += 1

        finally:
            db.close()

        if not did_work:
            time.sleep(poll_seconds)


if __name__ == "__main__":
    main()
