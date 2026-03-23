from __future__ import annotations

import json
import os
import shutil
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Literal, Tuple

from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy import inspect, text
from sqlalchemy.orm import Session

from app.db import get_db
from app.models.user import User
from app.routers.auth import get_current_user
from app.services.exporter import ExportRequest, build_export_zip_to_file

router = APIRouter(prefix="/admin/maintenance", tags=["AdminMaintenance"])


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def require_admin(current_user: User = Depends(get_current_user)) -> User:
    if not getattr(current_user, "is_admin", False):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin privileges required")
    return current_user


def _uploaded_logs_dir() -> str:
    env = (os.getenv("UPLOADED_LOGS_DIR") or "").strip()
    candidates = [
        env.rstrip("/") if env else "",
        "/app/app/uploaded_logs",
        "/app/uploaded_logs",
        os.path.abspath(os.path.join(os.getcwd(), "app", "uploaded_logs")),
    ]
    for c in candidates:
        if c and os.path.isdir(c):
            return c.rstrip("/")
    return "/app/app/uploaded_logs"


def _cleanup_file(path: str) -> None:
    try:
        os.remove(path)
    except Exception:
        pass


# -----------------------------
# CLEAN UPLOADED LOGS
# -----------------------------

class CleanResult(BaseModel):
    removed_files: int = 0
    removed_dirs: int = 0
    errors: List[str] = Field(default_factory=list)


def _count_files_recursive(path: str) -> int:
    total = 0
    for _root, _dirs, files in os.walk(path):
        total += len(files)
    return total


@router.post("/clean-uploaded-logs", response_model=CleanResult)
def clean_uploaded_logs(
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
) -> CleanResult:
    base = _uploaded_logs_dir()
    if not os.path.isdir(base):
        return CleanResult(removed_files=0, removed_dirs=0, errors=[f"Base dir not found: {base}"])

    result = CleanResult()

    try:
        entries = list(os.scandir(base))
    except Exception as e:
        return CleanResult(removed_files=0, removed_dirs=0, errors=[f"Cannot scan base dir {base}: {e}"])

    for entry in entries:
        p = entry.path
        try:
            if entry.is_symlink():
                os.unlink(p)
                result.removed_files += 1
                continue

            if entry.is_file():
                os.remove(p)
                result.removed_files += 1
                continue

            if entry.is_dir():
                result.removed_files += _count_files_recursive(p)
                shutil.rmtree(p)
                result.removed_dirs += 1
                continue

            os.remove(p)
            result.removed_files += 1

        except Exception as e:
            result.errors.append(f"Failed to remove {p}: {e}")

    return result


# -----------------------------
# EXPORT JOBS
# -----------------------------

class ExportPayload(BaseModel):
    days: int = Field(30, ge=1, le=3650)
    include: List[str] = Field(default_factory=lambda: ["events_excel", "alerts_excel", "incidents", "entities"])
    alert_dispositions: Optional[List[str]] = None
    incident_dispositions: Optional[List[str]] = None
    statuses: Optional[List[str]] = None


ExportJobStatus = Literal["queued", "running", "done", "error"]


class ExportJob(BaseModel):
    job_id: str
    status: ExportJobStatus
    created_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    filename: Optional[str] = None
    size_bytes: Optional[int] = None
    error: Optional[str] = None
    payload: ExportPayload


class ExportJobList(BaseModel):
    total: int
    items: List[ExportJob]


def _backups_dir() -> str:
    return (os.getenv("BACKUPS_DIR") or "/app/backups").rstrip("/")


def _exports_dir() -> str:
    return os.path.join(_backups_dir(), "exports")


def _jobs_dir() -> str:
    return os.path.join(_exports_dir(), "jobs")


def _ensure_dirs() -> None:
    os.makedirs(_exports_dir(), exist_ok=True)
    os.makedirs(_jobs_dir(), exist_ok=True)


def _job_path(job_id: str) -> str:
    return os.path.join(_jobs_dir(), f"{job_id}.json")


def _zip_path(job_id: str) -> str:
    return os.path.join(_exports_dir(), f"{job_id}.zip")


def _read_job(job_id: str) -> Dict[str, Any]:
    p = _job_path(job_id)
    if not os.path.isfile(p):
        raise HTTPException(status_code=404, detail="Export job not found")
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_job(job_id: str, data: Dict[str, Any]) -> None:
    p = _job_path(job_id)
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
    os.replace(tmp, p)


def _parse_dt(v: Optional[str]) -> Optional[datetime]:
    if not v:
        return None
    return datetime.fromisoformat(v)


def _job_to_model(data: Dict[str, Any]) -> ExportJob:
    payload = ExportPayload(**data["payload"])
    return ExportJob(
        job_id=data["job_id"],
        status=data["status"],
        created_at=datetime.fromisoformat(data["created_at"]),
        started_at=_parse_dt(data.get("started_at")),
        finished_at=_parse_dt(data.get("finished_at")),
        filename=data.get("filename"),
        size_bytes=data.get("size_bytes"),
        error=data.get("error"),
        payload=payload,
    )


def _list_job_files() -> List[Tuple[str, str]]:
    """
    Returns list of (job_id, path_to_job_json)
    """
    _ensure_dirs()
    out: List[Tuple[str, str]] = []
    for name in os.listdir(_jobs_dir()):
        if not name.endswith(".json"):
            continue
        job_id = name[:-5]
        out.append((job_id, os.path.join(_jobs_dir(), name)))
    return out


def _run_export_job(job_id: str, payload: ExportPayload) -> None:
    _ensure_dirs()

    job = _read_job(job_id)
    job["status"] = "running"
    job["started_at"] = _utc_now().isoformat()
    _write_job(job_id, job)

    include = [x.lower().strip() for x in (payload.include or []) if str(x).strip()]
    zip_path = _zip_path(job_id)

    try:
        # Abrimos una sesión DB NUEVA dentro del job
        from app.db import SessionLocal  # debe existir en tu app/db.py
        db = SessionLocal()
        try:
            with open(zip_path, "wb") as f:
                build_export_zip_to_file(
                    db,
                    ExportRequest(
                        days=int(payload.days),
                        include=include,
                        alert_dispositions=payload.alert_dispositions,
                        incident_dispositions=payload.incident_dispositions,
                        statuses=payload.statuses,
                    ),
                    f,
                )
        finally:
            db.close()

        size = os.path.getsize(zip_path) if os.path.isfile(zip_path) else None

        job = _read_job(job_id)
        job["status"] = "done"
        job["finished_at"] = _utc_now().isoformat()
        # Nombre amigable para el download
        job["filename"] = f"sentinelx_export_{job_id}.zip"
        job["size_bytes"] = size
        job["error"] = None
        _write_job(job_id, job)

    except Exception as e:
        _cleanup_file(zip_path)
        job = _read_job(job_id)
        job["status"] = "error"
        job["finished_at"] = _utc_now().isoformat()
        job["error"] = f"{type(e).__name__}: {e}"
        _write_job(job_id, job)


@router.post("/exports", response_model=ExportJob, status_code=202)
def create_export_job(
    payload: ExportPayload,
    background_tasks: BackgroundTasks,
    _: User = Depends(require_admin),
) -> ExportJob:
    _ensure_dirs()
    job_id = uuid.uuid4().hex

    job = {
        "job_id": job_id,
        "status": "queued",
        "created_at": _utc_now().isoformat(),
        "started_at": None,
        "finished_at": None,
        "filename": None,
        "size_bytes": None,
        "error": None,
        "payload": payload.model_dump(),
    }
    _write_job(job_id, job)

    background_tasks.add_task(_run_export_job, job_id, payload)

    return _job_to_model(_read_job(job_id))


@router.get("/exports", response_model=ExportJobList)
def list_export_jobs(
    limit: int = Query(30, ge=1, le=200),
    offset: int = Query(0, ge=0),
    _: User = Depends(require_admin),
) -> ExportJobList:
    files = _list_job_files()

    # Cargar jobs y ordenar por created_at desc
    jobs: List[ExportJob] = []
    for _job_id, path in files:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            jobs.append(_job_to_model(data))
        except Exception:
            # Si un job file está corrupto, lo ignoramos (no rompas el listing)
            continue

    jobs.sort(key=lambda j: j.created_at, reverse=True)

    total = len(jobs)
    items = jobs[offset: offset + limit]
    return ExportJobList(total=total, items=items)


@router.get("/exports/{job_id}", response_model=ExportJob)
def get_export_job(
    job_id: str,
    _: User = Depends(require_admin),
) -> ExportJob:
    return _job_to_model(_read_job(job_id))


@router.get("/exports/{job_id}/download")
def download_export_job(
    job_id: str,
    _: User = Depends(require_admin),
):
    data = _read_job(job_id)
    if data.get("status") != "done":
        raise HTTPException(status_code=409, detail=f"Export not ready (status={data.get('status')})")

    path = _zip_path(job_id)
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="Export file not found")

    filename = data.get("filename") or f"sentinelx_export_{job_id}.zip"

    return FileResponse(
        path=path,
        media_type="application/zip",
        filename=filename,
    )


# Compatibilidad: endpoint viejo ahora crea job y regresa 202
class ExportZipAccepted(BaseModel):
    job_id: str
    status: ExportJobStatus


@router.post("/export.zip", response_model=ExportZipAccepted, status_code=202)
def export_zip(
    payload: ExportPayload,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),   # no usado aquí pero mantiene patrón de dependencia
    _: User = Depends(require_admin),
) -> ExportZipAccepted:
    job = create_export_job(payload, background_tasks, _)
    return ExportZipAccepted(job_id=job.job_id, status=job.status)


# -----------------------------
# BACKUP AND WIPE DB
# -----------------------------

class BackupWipePayload(BaseModel):
    export_days: int = Field(30, ge=1, le=3650)


class BackupWipeResult(BaseModel):
    backup_path: str
    wiped_tables_count: int


@router.post("/backup-and-wipe-db", response_model=BackupWipeResult)
def backup_and_wipe_db(
    payload: BackupWipePayload,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
) -> BackupWipeResult:
    backups_dir = _backups_dir()
    os.makedirs(backups_dir, exist_ok=True)

    name = f"sentinelx_backup_{_utc_now().strftime('%Y%m%d_%H%M%S')}.zip"
    path = os.path.join(backups_dir, name)

    try:
        with open(path, "wb") as f:
            build_export_zip_to_file(
                db,
                ExportRequest(
                    days=int(payload.export_days),
                    include=["events", "alerts", "incidents", "entities", "rules_v2", "incident_rules"],
                ),
                f,
            )
    except Exception as e:
        _cleanup_file(path)
        raise HTTPException(status_code=500, detail=f"Backup export failed: {type(e).__name__}: {e}")

    keep: Set[str] = {
        "alembic_version",
        "users",
        "api_keys",
        "user_settings",
        "system_settings",
        "rules_v2",
        "rule_states_v2",
        "incident_rules",
        "incident_rule_state",
        "incident_rule_states",
    }

    insp = inspect(db.bind)
    tables = [t for t in insp.get_table_names() if t not in keep]

    if not tables:
        return BackupWipeResult(backup_path=path, wiped_tables_count=0)

    sql = "TRUNCATE " + ", ".join([f'"{t}"' for t in tables]) + " RESTART IDENTITY CASCADE"

    lock_timeout = os.getenv("DB_WIPE_LOCK_TIMEOUT", "10s")
    statement_timeout = os.getenv("DB_WIPE_STATEMENT_TIMEOUT", "15min")

    try:
        with db.begin():
            db.execute(text("SET LOCAL lock_timeout = :v"), {"v": lock_timeout})
            db.execute(text("SET LOCAL statement_timeout = :v"), {"v": statement_timeout})
            db.execute(text(sql))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=(
                "Backup generado, pero el wipe falló. "
                f"backup_path={path} err={type(e).__name__}: {type(e).__name__}: {e}"
            ),
        )

    return BackupWipeResult(backup_path=path, wiped_tables_count=int(len(tables)))
