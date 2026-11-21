"""关窗落库与回执一致性失败测（先红后改，口径见 ops/关窗落库册.md）。"""
import json
import sqlite3

import pytest
from fastapi.testclient import TestClient

import db
import windows_svc
from app import app, init_db


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("db.DB_PATH", tmp_path / "close.db")
    init_db()
    yield


def _login(c: TestClient, username: str, password: str) -> dict:
    tok = c.post(
        "/api/login", json={"username": username, "password": password}
    ).json()["token"]
    return {"Authorization": "Bearer " + tok}


def _window_row(wid: int) -> dict:
    with db.connect() as conn:
        return dict(conn.execute("SELECT * FROM windows WHERE id=?", (wid,)).fetchone())


def _audits(c: TestClient, h: dict) -> list[dict]:
    return c.get("/api/audit?limit=200", headers=h).json()["items"]


def test_close_receipt_matches_state():
    """关窗回执成功时，库内 status/seal_token 必须同步；后续入账不得再进窗内。"""
    c = TestClient(app)
    h = _login(c, "admin", "123456")

    r = c.post("/api/windows/2/close", headers=h)
    assert r.status_code == 200
    receipt = r.json()
    token = receipt["seal_token"]
    assert receipt["ok"] is True
    assert token

    # 落库：回执成功即状态已关闭、令牌一致
    row = _window_row(2)
    assert row["status"] == "已关闭"
    assert row["seal_token"] == token
    assert receipt.get("status") == row["status"]

    # 窗口条带
    strip = {
        w["id"]: w for w in c.get("/api/windows", headers=h).json()["items"]
    }
    assert strip[2]["status"] == "已关闭"
    assert strip[2]["seal_token"] == token

    # 窗口详情
    detail = c.get("/api/windows/2", headers=h).json()["window"]
    assert detail["status"] == "已关闭"
    assert detail["seal_token"] == token

    # 审计与回执同态
    closes = [
        a
        for a in _audits(c, h)
        if a["action"] == "window_close" and a["target"] == "2"
    ]
    assert len(closes) == 1
    assert json.loads(closes[0]["payload"])["seal_token"] == token

    # 关窗后入账只许进迟到桶，不得继续进窗内
    late = c.post(
        "/api/events",
        headers=h,
        json={
            "window_name": "W-1102",
            "biz_key": "AFTER-CLOSE",
            "event_ts": "2026-11-02T12:00:00",
            "amount": 5,
        },
    )
    assert late.status_code == 200
    assert late.json()["disposition"] == "迟到桶"

    # 重复关窗幂等：沿用既有令牌，不生成第二枚
    again = c.post("/api/windows/2/close", headers=h)
    assert again.status_code == 200
    assert again.json()["already"] is True
    assert again.json()["seal_token"] == token
    assert _window_row(2)["seal_token"] == token


def test_close_on_seeded_closed_window_is_idempotent():
    """种子行已关闭但无令牌（W-1101）：幂等回执，状态与令牌字段均零改。"""
    c = TestClient(app)
    h = _login(c, "admin", "123456")

    r = c.post("/api/windows/1/close", headers=h)
    assert r.status_code == 200
    body = r.json()
    assert body["already"] is True
    assert body["status"] == "已关闭"
    assert body["seal_token"] is None

    row = _window_row(1)
    assert row["status"] == "已关闭"
    assert row["seal_token"] is None


def test_double_close_contention():
    """两人同时关同一窗：先到占窗锁；后到只记争用，不写业务、不生成令牌。"""
    c = TestClient(app)
    h_admin = _login(c, "admin", "123456")
    h_closer = _login(c, "closer", "close123")

    # 先到者已占窗锁（等价于其关窗流程进入临界区）
    assert (
        c.post(
            "/api/locks/acquire",
            headers=h_admin,
            json={"lock_key": "window:2"},
        ).status_code
        == 200
    )

    # 后到者关窗：只记争用
    late = c.post("/api/windows/2/close", headers=h_closer)
    assert late.status_code == 409
    assert "seal_token" not in late.json()

    # 后到者不得挤掉/释放先到者的锁
    locks = {
        x["lock_key"]: x
        for x in c.get("/api/locks", headers=h_admin).json()["items"]
    }
    assert locks["window:2"]["holder"] == "admin"

    # 争用落库并进审计
    contention = [
        x
        for x in c.get("/api/contention", headers=h_admin).json()["items"]
        if x["lock_key"] == "window:2" and x["actor"] == "closer"
    ]
    assert contention
    assert any(
        a["action"] == "window_close_contention"
        for a in _audits(c, h_admin)
    )

    # 后到者零业务写：状态与令牌都未被改
    row = _window_row(2)
    assert row["status"] == "开启"
    assert row["seal_token"] is None

    # 先到者继续关窗成功，且只有一枚令牌
    won = c.post("/api/windows/2/close", headers=h_admin)
    assert won.status_code == 200
    token = won.json()["seal_token"]
    row = _window_row(2)
    assert row["status"] == "已关闭"
    assert row["seal_token"] == token
    closes = [
        a
        for a in _audits(c, h_admin)
        if a["action"] == "window_close" and a["target"] == "2"
    ]
    assert len(closes) == 1
    assert json.loads(closes[0]["payload"])["seal_token"] == token


def test_late_closer_after_lock_released_still_contention():
    """赢家已关账并释锁后，另一个人才打到关窗：仍须只记争用，不得拿成功回执。"""
    c = TestClient(app)
    h_admin = _login(c, "admin", "123456")
    h_closer = _login(c, "closer", "close123")

    won = c.post("/api/windows/2/close", headers=h_admin)
    assert won.status_code == 200
    token = won.json()["seal_token"]

    # 锁已释放，后到者仍不得以「已关账」为由拿 200
    assert c.get("/api/locks", headers=h_closer).json()["items"] == []
    late = c.post("/api/windows/2/close", headers=h_closer)
    assert late.status_code == 409
    assert "seal_token" not in late.json() or late.json().get("seal_token") != token
    assert _window_row(2)["seal_token"] == token

    contention = [
        x
        for x in c.get("/api/contention", headers=h_closer).json()["items"]
        if x["lock_key"] == "window:2" and x["actor"] == "closer"
    ]
    assert contention
    assert any(
        a["action"] == "window_close_contention"
        for a in _audits(c, h_closer)
    )

    # 关账人自己重放：幂等成功，令牌不变
    again = c.post("/api/windows/2/close", headers=h_admin).json()
    assert again["already"] is True
    assert again["seal_token"] == token


def test_close_ledger_deviation_rejects(tmp_path, monkeypatch):
    """代码路径偏离关窗落库册：拒写并记「关窗偏了」。"""
    c = TestClient(app)
    h = _login(c, "admin", "123456")

    bad = tmp_path / "关窗落库册.md"
    bad.write_text("# 关窗落库册\n\n选定路径：乙\n", encoding="utf-8")
    monkeypatch.setattr("audit_svc.CLOSE_LEDGER_PATH", bad, raising=False)

    r = c.post("/api/windows/3/close", headers=h)
    assert r.status_code == 409
    assert "seal_token" not in r.json()

    row = _window_row(3)
    assert row["status"] == "开启"
    assert row["seal_token"] is None
    assert any(
        a["action"] == "关窗偏了"
        for a in _audits(c, h)
    )


def _patch_connection(monkeypatch, factory):
    """sqlite3.Connection 是不可变类型，只能经 connect(factory=...) 替换。

    返回只还原该处的 undo；不要用 monkeypatch.undo()，否则会连带撤销
    autouse fixture 的 db.DB_PATH，使断言读到真实 data/app.db。
    """
    real_connect = sqlite3.connect

    def patched(*args, **kwargs):
        kwargs["factory"] = factory
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", patched)

    def undo():
        monkeypatch.setattr(sqlite3, "connect", real_connect)

    return undo


def test_close_commit_failure_yields_no_success_receipt(monkeypatch):
    """甲路径：落库事务提交失败，回执不得成功、不得带令牌，库内零改。"""
    c = TestClient(app, raise_server_exceptions=False)
    h = _login(c, "admin", "123456")

    state = {"armed": True}

    class FlakyConn(sqlite3.Connection):
        def commit(self):
            # 只让关窗业务事务（占锁提交之后、状态已改的那次）失败一次
            if state["armed"] and self.in_transaction:
                row = self.execute(
                    "SELECT status FROM windows WHERE id=2"
                ).fetchone()
                if row is not None and row["status"] == "已关闭":
                    state["armed"] = False
                    raise sqlite3.OperationalError("disk I/O error（注入）")
            return super().commit()

    undo = _patch_connection(monkeypatch, FlakyConn)

    r = c.post("/api/windows/2/close", headers=h)
    undo()

    assert r.status_code >= 500
    assert "seal_token" not in r.text
    assert not r.headers.get("x-seal-token")
    row = _window_row(2)
    assert row["status"] == "开启"
    assert row["seal_token"] is None
    assert not any(a["action"] == "window_close" for a in _audits(c, h))
    # 失败路径不得留锁
    assert all(
        x["lock_key"] != "window:2"
        for x in c.get("/api/locks", headers=h).json()["items"]
    )


def test_close_swallowed_update_rowcount_rejects(monkeypatch):
    """UPDATE 未命中状态位（例如被并发改走），不得发成功回执。"""
    c = TestClient(app)
    h = _login(c, "admin", "123456")

    class NoopUpdateConn(sqlite3.Connection):
        def execute(self, sql, *args):
            if isinstance(sql, str) and "UPDATE windows SET status='已关闭'" in sql:
                class _Cur:
                    rowcount = 0

                return _Cur()
            return super().execute(sql, *args)

    undo = _patch_connection(monkeypatch, NoopUpdateConn)
    r = c.post("/api/windows/3/close", headers=h)
    undo()

    assert r.status_code == 409
    assert "seal_token" not in r.json()
    row = _window_row(3)
    assert row["status"] == "开启"
    assert row["seal_token"] is None
    assert any(
        a["action"] == "window_close_contention"
        for a in _audits(c, h)
    )
