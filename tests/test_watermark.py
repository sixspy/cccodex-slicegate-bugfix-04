
import threading

import pytest
from fastapi.testclient import TestClient
from app import app, init_db


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    init_db()
    yield


def _login(c, username="admin", password="123456"):
    r = c.post("/api/login", json={"username": username, "password": password})
    assert r.status_code == 200
    return {"Authorization": f"Bearer {r.json()['token']}"}


def _mk_slice(c, h, slice_no, biz_date="2026-11-05", partition="east"):
    r = c.post("/api/slices", headers=h, json={
        "slice_no": slice_no, "biz_date": biz_date, "partition_key": partition,
        "row_count": 10, "expected_count": 10})
    assert r.status_code == 200
    return r.json()["id"]


def _current(c, h, consumer, biz_date="2026-11-05"):
    items = c.get("/api/watermarks", headers=h).json()["items"]
    return [w for w in items if w["consumer"] == consumer and w["biz_date"] == biz_date]


def test_replace_keeps_single_current_and_source_slices():
    """乙约束：后一次成功放行一次替换现行；旧水位进只读历史；新旧来源切片号都不丢。"""
    c = TestClient(app)
    h = _login(c)
    s1 = _mk_slice(c, h, "SL-W-1")
    s2 = _mk_slice(c, h, "SL-W-2", partition="west")
    assert c.post(f"/api/slices/{s1}/release", headers=h, json={"consumer": "ads_t"}).status_code == 200
    assert c.post(f"/api/slices/{s2}/release", headers=h, json={"consumer": "ads_t"}).status_code == 200

    cur = _current(c, h, "ads_t")
    assert len(cur) == 1, "同消费方同业务日正式水位最多一条现行"
    assert cur[0]["status"] == "现行"
    assert cur[0]["source_slice"] == "SL-W-2"

    hist = c.get("/api/watermarks/history?consumer=ads_t&biz_date=2026-11-05", headers=h).json()["items"]
    assert len(hist) == 2
    old = [w for w in hist if w["status"] == "历史"]
    assert len(old) == 1 and old[0]["source_slice"] == "SL-W-1", "替换不得丢掉旧来源切片号"

    events = c.get("/api/watermark-events", headers=h).json()["items"]
    assert any(e["kind"] == "水位争用禁默成" for e in events), "后手替换必须留痕，禁默成"


def test_concurrent_duty_officers_queue_and_second_is_recorded():
    """两名值班同时推同消费方同业务日水位：排队串行，均成功，只留一条现行，后手记争用。"""
    c = TestClient(app)
    h = _login(c)
    ids = [_mk_slice(c, h, f"SL-C-{i}", partition=f"p{i}") for i in range(2)]
    results = []

    def go(sid):
        r = c.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": "ads_c"})
        results.append(r.status_code)

    threads = [threading.Thread(target=go, args=(sid,)) for sid in ids]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(results) == [200, 200]
    assert len(_current(c, h, "ads_c")) == 1
    events = c.get("/api/watermark-events", headers=h).json()["items"]
    assert any(e["kind"] == "水位争用禁默成" for e in events)


def test_unique_current_constraint_holds_under_direct_double_insert():
    """库级唯一现行约束：绕过服务直插第二条现行也会被拒。"""
    import sqlite3
    from app import DB_PATH
    c = TestClient(app)
    h = _login(c)
    sid = _mk_slice(c, h, "SL-U-1")
    assert c.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": "ads_u"}).status_code == 200
    conn = sqlite3.connect(DB_PATH)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO watermarks(consumer,biz_date,watermark,source_slice,status,actor,updated_at) "
            "VALUES ('ads_u','2026-11-05','2026-11-05T01:00:00','SL-X','现行','x','2026-11-05T01:00:00')")
    conn.close()


def test_policy_registered_and_locked_same_day():
    """水位唯一册：口径已登记为乙约束；当天改册被拒并记「水位口径偏了」。"""
    c = TestClient(app)
    h = _login(c)
    policy = c.get("/api/watermark-policy", headers=h).json()
    assert "乙约束" in policy["standard"]

    r = c.put("/api/watermark-policy", headers=h, json={"standard": "甲约束·存在即拒绝"})
    assert r.status_code == 409
    assert "水位口径偏了" in r.json()["detail"]

    events = c.get("/api/watermark-events", headers=h).json()["items"]
    assert any(e["kind"] == "水位口径偏了" for e in events)

    # 幂等重登记同一口径不算换册
    ok = c.put("/api/watermark-policy", headers=h, json={"standard": policy["standard"]})
    assert ok.status_code == 200


def test_directory_listing_and_history_are_same_source():
    """消费目录、水位列表、历史详情同源可对：目录现行 == 列表现行 == 历史详情中的现行。"""
    c = TestClient(app)
    h = _login(c)
    sid = _mk_slice(c, h, "SL-D-1")
    assert c.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": "ads_d"}).status_code == 200

    cur = _current(c, h, "ads_d")
    assert len(cur) == 1
    directory = c.get("/api/consumers", headers=h).json()["items"]
    entry = [d for d in directory if d["consumer"] == "ads_d"]
    assert len(entry) == 1
    day = [w for w in entry[0]["current"] if w["biz_date"] == "2026-11-05"]
    assert len(day) == 1
    assert day[0]["watermark"] == cur[0]["watermark"]
    assert day[0]["source_slice"] == cur[0]["source_slice"]

    hist = c.get("/api/watermarks/history?consumer=ads_d&biz_date=2026-11-05", headers=h).json()["items"]
    current_in_hist = [w for w in hist if w["status"] == "现行"]
    assert len(current_in_hist) == 1
    assert current_in_hist[0]["watermark"] == cur[0]["watermark"]
