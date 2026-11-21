"""窗口锁与争用登记（公共并发面，题面争用句应对得上）。"""
from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from auth import AuthHeader, require_user
from db import connect, write_audit

router = APIRouter(tags=["locks"])


class LockIn(BaseModel):
    lock_key: str
    action: str = "acquire"


def record_contention(conn, lock_key: str, holder: str, detail: str) -> None:
    """争用留痕唯一写法：锁竞争与状态位竞争都经这里落 contention_log。"""
    now = datetime.now().isoformat(timespec="seconds")
    conn.execute(
        """INSERT INTO contention_log(lock_key,actor,action,detail,created_at)
           VALUES (?,?,?,?,?)""",
        (lock_key, holder, "争用", detail, now),
    )


def try_acquire(conn, lock_key: str, holder: str) -> bool:
    """写优先占锁：INSERT OR IGNORE 先取得写锁，再读持有者，避免读后写竞态。"""
    now = datetime.now().isoformat(timespec="seconds")
    conn.execute(
        "INSERT OR IGNORE INTO window_locks(lock_key,holder,acquired_at) VALUES (?,?,?)",
        (lock_key, holder, now),
    )
    row = conn.execute(
        "SELECT holder FROM window_locks WHERE lock_key=?", (lock_key,)
    ).fetchone()
    if row is not None and row["holder"] == holder:
        return True
    record_contention(
        conn, lock_key, holder, f"持有者={row['holder'] if row else '无'}"
    )
    return False


def release(conn, lock_key: str, holder: str) -> None:
    """只允许持有者释锁：后到者/过期流程不得抹掉先到者的窗锁。"""
    conn.execute(
        "DELETE FROM window_locks WHERE lock_key=? AND holder=?",
        (lock_key, holder),
    )


def require_lock(conn, lock_key: str, holder: str) -> None:
    if not try_acquire(conn, lock_key, holder):
        # 争用是既成事实，必须独立落库，不能随 409 异常回滚
        conn.commit()
        raise HTTPException(409, "争用：后到只记争用，不写业务")


@router.get("/api/locks")
def list_locks(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM window_locks ORDER BY acquired_at DESC"
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@router.get("/api/contention")
def list_contention(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM contention_log ORDER BY id DESC LIMIT 100"
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@router.post("/api/locks/acquire")
def acquire_lock(body: LockIn, authorization: AuthHeader = None):
    user = require_user(authorization)
    with connect() as conn:
        ok = try_acquire(conn, body.lock_key, user)
        write_audit(
            conn,
            user,
            "lock_acquire" if ok else "lock_contention",
            body.lock_key,
            {"ok": ok},
        )
        conn.commit()
        if not ok:
            raise HTTPException(409, "争用：后到只记争用")
        return {"ok": True, "lock_key": body.lock_key, "holder": user}


@router.post("/api/locks/release")
def release_lock(body: LockIn, authorization: AuthHeader = None):
    user = require_user(authorization)
    with connect() as conn:
        release(conn, body.lock_key, user)
        write_audit(conn, user, "lock_release", body.lock_key, {})
        conn.commit()
    return {"ok": True}
