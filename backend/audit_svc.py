"""审计与互证只读旁路；另含关窗落库册校验（唯一写入口径守卫）。"""
from __future__ import annotations

import re
from pathlib import Path

from fastapi import APIRouter, HTTPException

from auth import AuthHeader, require_user
from db import connect

router = APIRouter(tags=["audit"])

# 关窗落库册：代码口径必须与册一致，否则拒写关窗并记「关窗偏了」
CLOSE_LEDGER_PATH = Path(__file__).resolve().parents[1] / "ops" / "关窗落库册.md"
CLOSE_LEDGER_CHOICE = "甲"  # 本仓代码声明路径，落册当日锁定，禁止改口
CLOSE_DEVIATION_ACTION = "关窗偏了"


class CloseLedgerDeviation(Exception):
    """落库册缺失、被改写或选定路径与本仓代码不一致。"""


def require_close_ledger_choice() -> str:
    """读取关窗落库册；只认「选定路径：甲」，缺册或乙路径一律偏离。"""
    path = Path(CLOSE_LEDGER_PATH)
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise CloseLedgerDeviation(f"关窗落库册不可读：{path}") from exc
    selected = re.search(r"选定路径[：:]\s*([甲乙])", text)
    if not selected or selected.group(1) != CLOSE_LEDGER_CHOICE:
        found = selected.group(1) if selected else "未登记"
        raise CloseLedgerDeviation(
            f"落库册选定路径={found}，代码路径={CLOSE_LEDGER_CHOICE}，不得混用"
        )
    if "甲路径硬约束" not in text:
        raise CloseLedgerDeviation("落库册缺少甲路径硬约束段")
    return CLOSE_LEDGER_CHOICE


def reject_close_if_deviated(conn, wid: int, actor: str) -> None:
    """偏离落库册：记「关窗偏了」，拒写业务并返回 409。"""
    from db import write_audit

    try:
        require_close_ledger_choice()
    except CloseLedgerDeviation as exc:
        write_audit(
            conn,
            actor,
            CLOSE_DEVIATION_ACTION,
            str(wid),
            {"reason": str(exc), "rejected": True},
        )
        conn.commit()
        raise HTTPException(409, f"关窗落库册偏离，拒写：{exc}") from exc


@router.get("/api/audit")
def list_audit(authorization: AuthHeader = None, limit: int = 100):
    require_user(authorization)
    limit = max(1, min(limit, 500))
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM audit_trail ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@router.get("/api/audit/actions")
def audit_actions(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute(
            """SELECT action, COUNT(*) c FROM audit_trail
               GROUP BY action ORDER BY c DESC"""
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@router.get("/api/dashboard")
def dashboard(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        open_n = conn.execute(
            "SELECT COUNT(*) c FROM windows WHERE status='开启'"
        ).fetchone()["c"]
        closed_n = conn.execute(
            "SELECT COUNT(*) c FROM windows WHERE status='已关闭'"
        ).fetchone()["c"]
        late_n = conn.execute("SELECT COUNT(*) c FROM late_bucket").fetchone()["c"]
        reject_n = conn.execute("SELECT COUNT(*) c FROM reject_slips").fetchone()["c"]
        hold_n = conn.execute(
            "SELECT COUNT(*) c FROM hold_staging WHERE status='暂存'"
        ).fetchone()["c"]
        mats = conn.execute("SELECT COUNT(*) c FROM materializations").fetchone()["c"]
        contention = conn.execute("SELECT COUNT(*) c FROM contention_log").fetchone()["c"]
        return {
            "open_windows": open_n,
            "closed_windows": closed_n,
            "late_bucket": late_n,
            "rejects": reject_n,
            "holds": hold_n,
            "materializations": mats,
            "contention": contention,
        }
