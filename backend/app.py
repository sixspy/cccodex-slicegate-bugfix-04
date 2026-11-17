
import hashlib, secrets, sqlite3, threading
from datetime import date, datetime
from pathlib import Path
from fastapi import Header, HTTPException
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn

def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()

def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))

class LoginIn(BaseModel):
    username: str
    password: str

def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

PRODUCT = "日切放行"
SKELETON = "业务日分区切片登记后裁决完整度，完整才放行下游，并与消费水位对账"

# 水位唯一册登记口径（本服务唯一实现，择定后当天不许换）：
# 乙约束 —— 新成功放行以一次替换现行水位，旧水位进只读历史，页面默认只展示现行一条。
WM_STANDARD = "乙约束·新成功放行一次替换现行水位，旧水位进只读历史"
EV_CONTENTION = "水位争用禁默成"   # 后手替换前手必须留痕，禁止静默成功
EV_DEVIATION = "水位口径偏了"      # 偏离水位唯一册即拒并记

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# 同消费方同业务日推水位须排队：进程内按 (consumer, biz_date) 加锁，
# 库级再由 BEGIN IMMEDIATE 串行兜底。
_WM_LOCKS: dict[tuple[str, str], threading.Lock] = {}
_WM_LOCKS_GUARD = threading.Lock()

def _wm_lock(consumer: str, biz_date: str) -> threading.Lock:
    key = (consumer, biz_date)
    with _WM_LOCKS_GUARD:
        lk = _WM_LOCKS.get(key)
        if lk is None:
            lk = threading.Lock()
            _WM_LOCKS[key] = lk
        return lk

def _policy(conn):
    return conn.execute("SELECT * FROM watermark_policy WHERE id=1").fetchone()

def _wm_event(conn, consumer: str, biz_date: str, kind: str, detail: str, actor: str, now: str):
    conn.execute(
        "INSERT INTO watermark_events(consumer,biz_date,kind,detail,actor,created_at) VALUES (?,?,?,?,?,?)",
        (consumer, biz_date, kind, detail, actor, now),
    )

def _migrate_watermarks(conn):
    """旧库升级：补列 -> 修复存量双写（每消费方每业务日只留最新一条现行）-> 上唯一现行约束。"""
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(watermarks)").fetchall()}
    if "status" not in cols:
        conn.execute("ALTER TABLE watermarks ADD COLUMN status TEXT NOT NULL DEFAULT '现行'")
    if "actor" not in cols:
        conn.execute("ALTER TABLE watermarks ADD COLUMN actor TEXT NOT NULL DEFAULT ''")
    if "updated_at" not in cols:
        conn.execute("ALTER TABLE watermarks ADD COLUMN updated_at TEXT")
    conn.execute("""UPDATE watermarks SET status='历史'
                    WHERE status='现行' AND id NOT IN (
                      SELECT MAX(id) FROM watermarks WHERE status='现行' GROUP BY consumer, biz_date)""")
    conn.execute("UPDATE watermarks SET updated_at=watermark WHERE updated_at IS NULL")
    conn.execute("UPDATE watermarks SET actor='system' WHERE actor=''")
    # 唯一现行约束：同消费方同业务日正式水位最多一条现行（库级硬约束）
    conn.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ux_watermarks_one_current
                    ON watermarks(consumer, biz_date) WHERE status='现行'""")

def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS slices(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          slice_no TEXT UNIQUE NOT NULL,
          biz_date TEXT NOT NULL,
          partition_key TEXT NOT NULL,
          row_count INTEGER NOT NULL,
          expected_count INTEGER NOT NULL,
          completeness TEXT NOT NULL,
          release_status TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS releases(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          slice_id INTEGER NOT NULL,
          consumer TEXT NOT NULL,
          status TEXT NOT NULL,
          note TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS watermarks(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          consumer TEXT NOT NULL,
          biz_date TEXT NOT NULL,
          watermark TEXT NOT NULL,
          source_slice TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT '现行',
          actor TEXT NOT NULL DEFAULT '',
          updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS watermark_policy(
          id INTEGER PRIMARY KEY CHECK (id=1),
          standard TEXT NOT NULL,
          decided_on TEXT NOT NULL,
          decided_by TEXT NOT NULL,
          decided_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS watermark_events(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          consumer TEXT NOT NULL DEFAULT '',
          biz_date TEXT NOT NULL DEFAULT '',
          kind TEXT NOT NULL,
          detail TEXT NOT NULL,
          actor TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        """)
        _migrate_watermarks(conn)
        if conn.execute("SELECT COUNT(*) c FROM slices").fetchone()["c"] == 0:
            rows = [
                ("SL-2026-11-01-east", "2026-11-01", "east", 980, 1000, "部分完整", "未放行"),
                ("SL-2026-11-01-west", "2026-11-01", "west", 1000, 1000, "完整", "已放行"),
                ("SL-2026-11-02-east", "2026-11-02", "east", 0, 1000, "待检", "未放行"),
            ]
            for r in rows:
                conn.execute("INSERT INTO slices(slice_no,biz_date,partition_key,row_count,expected_count,completeness,release_status) VALUES (?,?,?,?,?,?,?)", r)
            conn.execute("INSERT INTO releases(slice_id,consumer,status,note,created_at) VALUES (2,'ads_order','成功','完整切片放行','2026-11-01T18:00:00')")
            conn.execute("""INSERT INTO watermarks(consumer,biz_date,watermark,source_slice,status,actor,updated_at)
                            VALUES ('ads_order','2026-11-01','2026-11-01T18:00:00','SL-2026-11-01-west','现行','system','2026-11-01T18:00:00')""")
        if conn.execute("SELECT COUNT(*) c FROM watermark_policy").fetchone()["c"] == 0:
            now = datetime.now().isoformat(timespec="seconds")
            conn.execute("INSERT INTO watermark_policy(id,standard,decided_on,decided_by,decided_at) VALUES (1,?,?,?,?)",
                         (WM_STANDARD, date.today().isoformat(), "system", now))

@app.on_event("startup")
def _start():
    init_db()

@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM slices").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "slices": n, "db": str(DB_PATH)}

@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username, role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row:
        raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16); SESSIONS[tok] = row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}

@app.get("/api/slices")
def list_slices(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM slices ORDER BY id").fetchall()
    return {"items": [dict(r) for r in rows]}

class SliceIn(BaseModel):
    slice_no: str
    biz_date: str
    partition_key: str
    row_count: int
    expected_count: int

@app.post("/api/slices")
def add_slice(body: SliceIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    comp = "完整" if body.row_count >= body.expected_count and body.expected_count > 0 else ("部分完整" if body.row_count > 0 else "待检")
    with connect() as c:
        try:
            cur = c.execute(
                "INSERT INTO slices(slice_no,biz_date,partition_key,row_count,expected_count,completeness,release_status) VALUES (?,?,?,?,?,?,?)",
                (body.slice_no, body.biz_date, body.partition_key, body.row_count, body.expected_count, comp, "未放行"),
            )
            return {"id": cur.lastrowid, "completeness": comp}
        except sqlite3.IntegrityError:
            raise HTTPException(409, "切片号已存在")

@app.post("/api/slices/{sid}/evaluate")
def evaluate(sid: int, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        row = c.execute("SELECT * FROM slices WHERE id=?", (sid,)).fetchone()
        if not row:
            raise HTTPException(404, "切片不存在")
        comp = "完整" if row["row_count"] >= row["expected_count"] and row["expected_count"] > 0 else ("部分完整" if row["row_count"] > 0 else "待检")
        c.execute("UPDATE slices SET completeness=? WHERE id=?", (comp, sid))
    return {"id": sid, "completeness": comp}

class ReleaseIn(BaseModel):
    consumer: str

@app.post("/api/slices/{sid}/release")
def release(sid: int, body: ReleaseIn, authorization: str | None = Header(default=None)):
    user = require_user(authorization)
    with connect() as c:
        peek = c.execute("SELECT id, biz_date FROM slices WHERE id=?", (sid,)).fetchone()
    if not peek:
        raise HTTPException(404, "切片不存在")
    lock = _wm_lock(body.consumer, peek["biz_date"])
    queued = lock.locked()  # 已有人持锁 -> 本次为后手，须排队
    with lock:
        conn = connect()
        conn.isolation_level = None
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM slices WHERE id=?", (sid,)).fetchone()
            if not row:
                conn.execute("ROLLBACK")
                raise HTTPException(404, "切片不存在")
            now = datetime.now().isoformat(timespec="seconds")
            if row["completeness"] != "完整":
                conn.execute("INSERT INTO releases(slice_id,consumer,status,note,created_at) VALUES (?,?,?,?,?)",
                             (sid, body.consumer, "拒绝", "完整度未达标", now))
                conn.execute("COMMIT")
                raise HTTPException(409, "未完整禁止放行")
            policy = _policy(conn)
            if not policy or policy["standard"] != WM_STANDARD:
                got = policy["standard"] if policy else "（空册）"
                _wm_event(conn, body.consumer, row["biz_date"], EV_DEVIATION,
                          f"水位唯一册登记「{got}」与服务实现「{WM_STANDARD}」不符，拒绝写入水位", user, now)
                conn.execute("COMMIT")
                raise HTTPException(409, "水位口径偏了：写法与水位唯一册不符，已拒绝并记录")
            cur = conn.execute(
                "SELECT * FROM watermarks WHERE consumer=? AND biz_date=? AND status='现行'",
                (body.consumer, row["biz_date"])).fetchone()
            if cur:
                # 乙约束：一次替换现行；旧水位进只读历史；新旧来源切片号都保留，不得弄丢
                conn.execute("UPDATE watermarks SET status='历史', updated_at=? WHERE id=?", (now, cur["id"]))
                detail = (f"后手「{user}」替换前手「{cur['actor'] or '未知'}」：现行来源切片 "
                          f"{cur['source_slice']} → {row['slice_no']}，旧水位进只读历史")
                if queued:
                    detail += "（写入前已排队等待）"
                _wm_event(conn, body.consumer, row["biz_date"], EV_CONTENTION, detail, user, now)
            conn.execute(
                "INSERT INTO watermarks(consumer,biz_date,watermark,source_slice,status,actor,updated_at) VALUES (?,?,?,?,'现行',?,?)",
                (body.consumer, row["biz_date"], now, row["slice_no"], user, now))
            conn.execute("UPDATE slices SET release_status='已放行' WHERE id=?", (sid,))
            conn.execute("INSERT INTO releases(slice_id,consumer,status,note,created_at) VALUES (?,?,?,?,?)",
                         (sid, body.consumer, "成功", "完整切片放行", now))
            conn.execute("COMMIT")
        except HTTPException:
            raise
        except sqlite3.IntegrityError:
            conn.execute("ROLLBACK")
            raise HTTPException(409, "违反唯一现行约束：同消费方同业务日正式水位最多一条现行")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            conn.close()
    return {"ok": True}

@app.get("/api/releases")
def list_releases(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("""SELECT r.*, s.slice_no FROM releases r JOIN slices s ON s.id=r.slice_id ORDER BY r.id DESC""").fetchall()
    return {"items": [dict(r) for r in rows]}

@app.get("/api/watermarks")
def list_wm(all: int = 0, authorization: str | None = Header(default=None)):
    """水位列表：默认只展示现行一条；all=1 时连只读历史一起给（对账用）。"""
    require_user(authorization)
    sql = "SELECT * FROM watermarks"
    if not all:
        sql += " WHERE status='现行'"
    sql += " ORDER BY id DESC"
    with connect() as c:
        rows = c.execute(sql).fetchall()
    return {"items": [dict(r) for r in rows]}

@app.get("/api/watermarks/history")
def wm_history(consumer: str, biz_date: str, authorization: str | None = Header(default=None)):
    """历史详情：同消费方同业务日的全部水位版本（现行 + 只读历史）。"""
    require_user(authorization)
    with connect() as c:
        rows = c.execute(
            "SELECT * FROM watermarks WHERE consumer=? AND biz_date=? ORDER BY id DESC",
            (consumer, biz_date)).fetchall()
    return {"items": [dict(r) for r in rows]}

@app.get("/api/consumers")
def list_consumers(authorization: str | None = Header(default=None)):
    """消费目录：与水位列表同源，取自同一张 watermarks 表的现行行。"""
    require_user(authorization)
    with connect() as c:
        rows = c.execute("""
            SELECT w.consumer, w.biz_date, w.watermark, w.source_slice, w.actor, w.updated_at,
                   (SELECT COUNT(*) FROM watermarks h WHERE h.consumer=w.consumer AND h.biz_date=w.biz_date) AS versions
            FROM watermarks w WHERE w.status='现行'
            ORDER BY w.consumer, w.biz_date DESC""").fetchall()
    directory: dict[str, dict] = {}
    for r in rows:
        d = directory.setdefault(r["consumer"], {"consumer": r["consumer"], "current": []})
        d["current"].append({k: r[k] for k in ("biz_date", "watermark", "source_slice", "actor", "updated_at", "versions")})
    return {"items": list(directory.values())}

@app.get("/api/watermark-policy")
def get_policy(authorization: str | None = Header(default=None)):
    """策略条数据源：水位唯一册登记的现行口径。"""
    require_user(authorization)
    with connect() as c:
        row = _policy(c)
    return dict(row) if row else {}

class PolicyIn(BaseModel):
    standard: str

@app.put("/api/watermark-policy")
def put_policy(body: PolicyIn, authorization: str | None = Header(default=None)):
    user = require_user(authorization)
    today = date.today().isoformat()
    now = datetime.now().isoformat(timespec="seconds")
    conn = connect()
    conn.isolation_level = None
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = _policy(conn)
        if row and row["standard"] == body.standard:
            conn.execute("COMMIT")
            return {"ok": True, "standard": row["standard"], "decided_on": row["decided_on"]}
        if row and row["decided_on"] == today:
            _wm_event(conn, "", "", EV_DEVIATION,
                      f"当天已选定「{row['standard']}」，「{user}」试图改册为「{body.standard}」，已拒绝", user, now)
            conn.execute("COMMIT")
            raise HTTPException(409, "水位口径偏了：当天不许换，水位唯一册已登记现行口径")
        if body.standard != WM_STANDARD:
            _wm_event(conn, "", "", EV_DEVIATION,
                      f"「{user}」请求登记本服务未实现的口径「{body.standard}」，已拒绝", user, now)
            conn.execute("COMMIT")
            raise HTTPException(409, "水位口径偏了：本服务仅实现水位唯一册登记口径")
        conn.execute(
            """INSERT INTO watermark_policy(id,standard,decided_on,decided_by,decided_at) VALUES (1,?,?,?,?)
               ON CONFLICT(id) DO UPDATE SET standard=excluded.standard, decided_on=excluded.decided_on,
                                             decided_by=excluded.decided_by, decided_at=excluded.decided_at""",
            (WM_STANDARD, today, user, now))
        conn.execute("COMMIT")
    except HTTPException:
        raise
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        conn.close()
    return {"ok": True, "standard": WM_STANDARD, "decided_on": today}

@app.get("/api/watermark-events")
def list_wm_events(authorization: str | None = Header(default=None)):
    """水位事件：争用禁默成 / 口径偏了 等留痕，与水位同源。"""
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM watermark_events ORDER BY id DESC LIMIT 50").fetchall()
    return {"items": [dict(r) for r in rows]}

@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")

app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
