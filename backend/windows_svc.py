"""业务窗开/关/列表。"""
from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException

from audit_svc import reject_close_if_deviated
from auth import AuthHeader, require_user
from db import connect, write_audit
from locks import record_contention, release, try_acquire
from models import WindowCreateIn

router = APIRouter(tags=["windows"])


@router.get("/api/windows")
def list_windows(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute("SELECT * FROM windows ORDER BY id").fetchall()
        return {"items": [dict(r) for r in rows]}


@router.post("/api/windows")
def create_window(body: WindowCreateIn, authorization: AuthHeader = None):
    user = require_user(authorization)
    with connect() as conn:
        try:
            conn.execute(
                "INSERT INTO windows(name,open_at,close_at,status,generation) VALUES (?,?,?,?,1)",
                (body.name, body.open_at, body.close_at, "开启"),
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(409, f"窗口创建失败: {exc}") from exc
        wid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        write_audit(conn, user, "window_create", body.name, {"id": wid})
        conn.commit()
        row = conn.execute("SELECT * FROM windows WHERE id=?", (wid,)).fetchone()
        return dict(row)


@router.post("/api/windows/{wid}/close")
def close_window(wid: int, authorization: AuthHeader = None):
    """关窗：甲路径——状态与 seal_token 同事务提交，提交成功才发回执。"""
    user = require_user(authorization)
    lock_key = f"window:{wid}"
    acquired = False
    with connect() as conn:
        try:
            if not try_acquire(conn, lock_key, user):
                write_audit(
                    conn,
                    user,
                    "window_close_contention",
                    str(wid),
                    {"lock_key": lock_key, "reason": "后到只记争用，不写业务"},
                )
                conn.commit()
                raise HTTPException(409, "争用：后到只记争用，不写业务")
            acquired = True
            # 占锁先独立提交：让真正重叠的后到者看到已占窗锁，只记争用
            conn.commit()

            # 关窗落库册偏离则拒写并记「关窗偏了」
            reject_close_if_deviated(conn, wid, user)

            try:
                w = conn.execute(
                    "SELECT * FROM windows WHERE id=?", (wid,)
                ).fetchone()
                if not w:
                    raise HTTPException(404, "窗口不存在")

                if w["status"] == "已关闭":
                    token = w["seal_token"]
                    sealer = conn.execute(
                        """SELECT actor FROM audit_trail
                           WHERE action='window_close' AND target=?
                           ORDER BY id DESC LIMIT 1""",
                        (str(wid),),
                    ).fetchone()
                    # 只有亲手关账的同一人可幂等重放；他人后到只记争用
                    if sealer is not None and sealer["actor"] != user:
                        record_contention(
                            conn,
                            lock_key,
                            user,
                            f"窗口已由 {sealer['actor']} 关账，后到只记争用",
                        )
                        write_audit(
                            conn,
                            user,
                            "window_close_contention",
                            str(wid),
                            {
                                "lock_key": lock_key,
                                "reason": f"已被 {sealer['actor']} 关账",
                                "seal_token": token,
                            },
                        )
                        conn.commit()
                        raise HTTPException(409, "争用：后到只记争用，不写业务")
                    write_audit(
                        conn,
                        user,
                        "window_close_replay",
                        str(wid),
                        {"seal_token": token, "generation": w["generation"]},
                    )
                    conn.commit()
                    return {
                        "ok": True,
                        "already": True,
                        "status": w["status"],
                        "seal_token": token,
                    }

                now = datetime.now().isoformat(timespec="seconds")
                token = f"seal-{wid}-{w['generation']}-{now}"

                # 甲路径：状态与令牌同一事务写入，回执前回读校验
                cur = conn.execute(
                    """UPDATE windows SET status='已关闭', seal_token=?
                       WHERE id=? AND status=? AND seal_token IS ?""",
                    (token, wid, "开启", w["seal_token"]),
                )
                if cur.rowcount != 1:
                    # 占锁成功但状态位被改走：仍按争用留痕，绝不发成功回执。
                    # UPDATE 未命中即零业务写，审计须独立提交后再拒绝。
                    write_audit(
                        conn,
                        user,
                        "window_close_contention",
                        str(wid),
                        {"lock_key": lock_key, "reason": "状态位已被改走"},
                    )
                    conn.commit()
                    raise HTTPException(409, "关窗未占到状态位，拒发成功回执")
                write_audit(
                    conn,
                    user,
                    "window_close",
                    str(wid),
                    {
                        "seal_token": token,
                        "from_status": w["status"],
                        "to_status": "已关闭",
                        "generation": w["generation"],
                    },
                )
                sealed = conn.execute(
                    "SELECT status, seal_token FROM windows WHERE id=?", (wid,)
                ).fetchone()
                if (
                    sealed["status"] != "已关闭"
                    or sealed["seal_token"] != token
                ):
                    raise HTTPException(500, "关窗落库与回执不一致，已回滚")
                conn.commit()
                return {
                    "ok": True,
                    "already": False,
                    "status": sealed["status"],
                    "seal_token": sealed["seal_token"],
                }
            except BaseException:
                # 业务事务未成功：先回滚再释锁，禁止把半成品随释锁提交
                conn.rollback()
                raise
        finally:
            if acquired:
                release(conn, lock_key, user)
                conn.commit()


@router.get("/api/windows/{wid}")
def get_window(wid: int, authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        w = conn.execute("SELECT * FROM windows WHERE id=?", (wid,)).fetchone()
        if not w:
            raise HTTPException(404, "窗口不存在")
        ev = conn.execute(
            "SELECT COUNT(*) c, COALESCE(SUM(amount),0) s FROM events WHERE window_id=? AND disposition='窗内'",
            (wid,),
        ).fetchone()
        return {
            "window": dict(w),
            "in_window_count": ev["c"],
            "in_window_amount": ev["s"],
        }
