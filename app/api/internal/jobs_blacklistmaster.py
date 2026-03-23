from __future__ import annotations

import os
from typing import Optional

from fastapi import APIRouter, Header, HTTPException

from app.services.blacklistmaster_sync import run_blacklistmaster_sync

router = APIRouter(
    prefix="/internal/jobs/blacklistmaster",
    tags=["internal-jobs"],
)


@router.post("/sync")
def sync_blacklistmaster(
    x_internal_token: Optional[str] = Header(default=None, alias="X-Internal-Token"),
):
    """
    Job interno: sincroniza estado de BlacklistMaster y genera alertas.

    Protegido por X-Internal-Token (shared secret).
    """
    expected = os.getenv("INTERNAL_JOBS_TOKEN", "").strip()
    if not expected:
        raise HTTPException(status_code=500, detail="Missing env INTERNAL_JOBS_TOKEN")

    if not x_internal_token or x_internal_token != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")

    return run_blacklistmaster_sync()

