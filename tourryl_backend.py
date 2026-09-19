# ============================================================
# tourryl_backend.py — KUMG / TouRryl — Postgres edition (v6)
# Migrated from SQLite to Supabase Postgres for Render deployment
# ============================================================
import os, sys, re, json, math, uuid, hashlib, secrets, asyncio
from datetime import datetime, timedelta
from collections import namedtuple

import psycopg2
from psycopg2.extras import RealDictCursor

from fastapi import (FastAPI, Depends, HTTPException, UploadFile, File, Header,
                     WebSocket, WebSocketDisconnect, Query, Request, Response)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse, RedirectResponse
from pydantic import BaseModel
import jwt, uvicorn

try: import httpx
except ImportError: httpx = None
try: import pyotp
except ImportError: pyotp = None

# ================= CONFIG =================
SECRET = os.environ.get("TOURRYL_SECRET", "kumg-tourryl-change-this-secret")
ALGO = "HS256"
UPLOAD_DIR = "uploads"
MAX_UPLOAD_MB = 100

PG_URL = os.environ.get("SUPABASE_PG_URL", "").strip()

STRIPE_KEY = os.environ.get("STRIPE_SECRET_KEY", "").strip()
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "").strip()
PUBLIC_URL = os.environ.get("PUBLIC_URL", "").rstrip("/")

RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "").strip()
EMAIL_FROM = os.environ.get("EMAIL_FROM", "TouRryl <onboarding@resend.dev>")
EMAIL_ENABLED = bool(RESEND_API_KEY)
REQUIRE_EMAIL_VERIFICATION = os.environ.get("REQUIRE_EMAIL_VERIFICATION", "0") == "1"
VERIFICATION_CODE_TTL_MIN = 15

if STRIPE_KEY:
    import stripe
    stripe.api_key = STRIPE_KEY

os.makedirs(UPLOAD_DIR, exist_ok=True)

app = FastAPI(title="TouRryl API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "PATCH", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=3600,
)

@app.options("/{full_path:path}")
async def preflight_handler(full_path: str):
    return Response(status_code=200)

app.mount("/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")

# ================= POSTGRES COMPAT WRAPPER =================
def _translate_sql(sql: str) -> str:
    sql = sql.replace("?", "%s")
    sql = re.sub(r"datetime\('now',\s*'([+-])(\d+)\s+(\w+)'\)",
                 r"(NOW() \1 INTERVAL '\2 \3')", sql)
    sql = re.sub(r"datetime\('now'\)", "NOW()", sql)
    sql = re.sub(r"MAX\(0,\s*", "GREATEST(0, ", sql)
    if "INSERT OR IGNORE INTO" in sql.upper():
        sql = re.sub(r"INSERT OR IGNORE INTO", "INSERT INTO", sql, flags=re.IGNORECASE)
        if "ON CONFLICT" not in sql.upper():
            sql = sql.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
    return sql

class DBCursor:
    def __init__(self, cur, lastrowid=None):
        self._cur = cur
        self._lastrowid = lastrowid
    def fetchone(self): return self._cur.fetchone()
    def fetchall(self): return self._cur.fetchall()
    def __iter__(self): return iter(self._cur)
    @property
    def rowcount(self): return self._cur.rowcount
    @property
    def lastrowid(self): return self._lastrowid

class DBConn:
    def __init__(self):
        self._conn = psycopg2.connect(PG_URL)
    def execute(self, sql, params=()):
        cur = self._conn.cursor(cursor_factory=RealDictCursor)
        sql2 = _translate_sql(sql)
        is_insert = sql2.strip().upper().startswith("INSERT")
        has_ret = "RETURNING" in sql2.upper()
        if is_insert and not has_ret:
            sql2 = sql2.rstrip().rstrip(";") + " RETURNING id"
        cur.execute(sql2, params)
        lastrowid = None
        if is_insert and not has_ret:
            try:
                row = cur.fetchone()
                if row and "id" in row: lastrowid = row["id"]
            except: pass
        return DBCursor(cur, lastrowid)
    def commit(self): self._conn.commit()
    def rollback(self): self._conn.rollback()
    def close(self): self._conn.close()
    def executescript(self, script):
        cur = self._conn.cursor()
        cur.execute(script)

def connect():
    return DBConn()

def _add_col(conn, table, column, decl):
    try:
        cur = conn.execute(
            "SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name=%s",
            (table, column)
        )
        if not cur.fetchone():
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")
            conn.commit()
    except Exception as e:
        print(f"[add_col] {table}.{column}: {e}")
        try: conn.rollback()
        except: pass

# ================= SCHEMA =================
def init_db():
    conn = connect()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS users(
        id BIGSERIAL PRIMARY KEY,
        username TEXT UNIQUE NOT NULL,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        bio TEXT DEFAULT '',
        avatar_url TEXT,
        followers_count INTEGER DEFAULT 0,
        following_count INTEGER DEFAULT 0,
        is_admin INTEGER DEFAULT 0,
        banned INTEGER DEFAULT 0,
        soft_banned INTEGER DEFAULT 0,
        strikes INTEGER DEFAULT 0,
        trust_score REAL DEFAULT 0,
        email_verified INTEGER DEFAULT 0,
        phone_verified INTEGER DEFAULT 0,
        phone TEXT,
        seller_rating REAL DEFAULT 0,
        seller_review_count INTEGER DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS user_details(
        user_id BIGINT PRIMARY KEY,
        age INTEGER, gender TEXT, country TEXT, city TEXT
    );
    CREATE TABLE IF NOT EXISTS settings(
        user_id BIGINT PRIMARY KEY,
        data_saver INTEGER DEFAULT 0,
        theme TEXT DEFAULT 'dark',
        language TEXT DEFAULT 'en',
        autoplay_video INTEGER DEFAULT 1
    );
    CREATE TABLE IF NOT EXISTS sessions(
        id TEXT PRIMARY KEY,
        user_id BIGINT NOT NULL,
        device TEXT, ip TEXT, user_agent TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        last_seen TIMESTAMPTZ DEFAULT NOW(),
        revoked INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS devices(
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        fingerprint TEXT NOT NULL,
        user_agent TEXT, screen TEXT, timezone TEXT, language TEXT, ip TEXT,
        blocked INTEGER DEFAULT 0,
        first_seen TIMESTAMPTZ DEFAULT NOW(),
        last_seen TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(user_id, fingerprint)
    );
    CREATE TABLE IF NOT EXISTS posts(
        id BIGSERIAL PRIMARY KEY,
        author_id BIGINT NOT NULL,
        content TEXT, media_url TEXT, media_type TEXT,
        likes_count INTEGER DEFAULT 0,
        comments_count INTEGER DEFAULT 0,
        views_count INTEGER DEFAULT 0,
        report_count INTEGER DEFAULT 0,
        hidden INTEGER DEFAULT 0,
        edited_at TIMESTAMPTZ,
        repost_of BIGINT,
        sound_name TEXT,
        listing_id BIGINT,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS likes(
        post_id BIGINT NOT NULL,
        user_id BIGINT NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (post_id, user_id)
    );
    CREATE TABLE IF NOT EXISTS comments(
        id BIGSERIAL PRIMARY KEY,
        post_id BIGINT NOT NULL,
        user_id BIGINT NOT NULL,
        parent_id BIGINT,
        body TEXT NOT NULL,
        likes_count INTEGER DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS comment_likes(
        comment_id BIGINT NOT NULL,
        user_id BIGINT NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (comment_id, user_id)
    );
    CREATE TABLE IF NOT EXISTS search_history(
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        query TEXT NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS post_views(
        id BIGSERIAL PRIMARY KEY,
        post_id BIGINT NOT NULL,
        user_id BIGINT,
        watch_seconds REAL DEFAULT 0,
        completed INTEGER DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS follows(
        follower_id BIGINT NOT NULL,
        following_id BIGINT NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (follower_id, following_id)
    );
    CREATE TABLE IF NOT EXISTS blocks(
        blocker_id BIGINT NOT NULL,
        blocked_id BIGINT NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (blocker_id, blocked_id)
    );
    CREATE TABLE IF NOT EXISTS saves(
        user_id BIGINT NOT NULL,
        item_type TEXT NOT NULL,
        item_id BIGINT NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (user_id, item_type, item_id)
    );
    CREATE TABLE IF NOT EXISTS stories(
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        media_url TEXT NOT NULL,
        media_type TEXT NOT NULL,
        caption TEXT,
        views_count INTEGER DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        expires_at TIMESTAMPTZ NOT NULL
    );
    CREATE TABLE IF NOT EXISTS story_views(
        story_id BIGINT NOT NULL,
        viewer_id BIGINT NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (story_id, viewer_id)
    );
    CREATE TABLE IF NOT EXISTS hashtags(
        id BIGSERIAL PRIMARY KEY,
        tag TEXT UNIQUE NOT NULL,
        post_count INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS post_hashtags(
        post_id BIGINT NOT NULL,
        hashtag_id BIGINT NOT NULL,
        PRIMARY KEY (post_id, hashtag_id)
    );
    CREATE TABLE IF NOT EXISTS conversations(
        id BIGSERIAL PRIMARY KEY,
        user_a BIGINT NOT NULL,
        user_b BIGINT NOT NULL,
        last_message_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(user_a, user_b)
    );
    CREATE TABLE IF NOT EXISTS messages(
        id BIGSERIAL PRIMARY KEY,
        conversation_id BIGINT NOT NULL,
        sender_id BIGINT NOT NULL,
        body TEXT NOT NULL,
        read INTEGER DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS notifications(
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        actor_id BIGINT NOT NULL,
        type TEXT NOT NULL,
        post_id BIGINT, comment_id BIGINT,
        read INTEGER DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS listings(
        id BIGSERIAL PRIMARY KEY,
        seller_id BIGINT NOT NULL,
        title TEXT NOT NULL,
        description TEXT,
        price REAL NOT NULL,
        currency TEXT DEFAULT 'USD',
        category TEXT DEFAULT 'other',
        condition TEXT DEFAULT 'used',
        media_url TEXT, media_type TEXT,
        location TEXT,
        status TEXT DEFAULT 'active',
        views_count INTEGER DEFAULT 0,
        stripe_enabled INTEGER DEFAULT 0,
        offers_enabled INTEGER DEFAULT 1,
        edited_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS listing_images(
        id BIGSERIAL PRIMARY KEY,
        listing_id BIGINT NOT NULL,
        media_url TEXT NOT NULL,
        media_type TEXT DEFAULT 'image',
        position INTEGER DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS offers(
        id BIGSERIAL PRIMARY KEY,
        listing_id BIGINT NOT NULL,
        buyer_id BIGINT NOT NULL,
        amount REAL NOT NULL,
        currency TEXT DEFAULT 'USD',
        message TEXT,
        status TEXT DEFAULT 'pending',
        counter_amount REAL,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        responded_at TIMESTAMPTZ
    );
    CREATE TABLE IF NOT EXISTS seller_reviews(
        id BIGSERIAL PRIMARY KEY,
        seller_id BIGINT NOT NULL,
        buyer_id BIGINT NOT NULL,
        listing_id BIGINT,
        rating INTEGER NOT NULL,
        comment TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS orders(
        id BIGSERIAL PRIMARY KEY,
        listing_id BIGINT NOT NULL,
        buyer_id BIGINT NOT NULL,
        seller_id BIGINT NOT NULL,
        amount REAL NOT NULL,
        currency TEXT NOT NULL,
        status TEXT DEFAULT 'pending',
        stripe_session_id TEXT UNIQUE,
        stripe_payment_intent TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        paid_at TIMESTAMPTZ
    );
    CREATE TABLE IF NOT EXISTS reports(
        id BIGSERIAL PRIMARY KEY,
        reporter_id BIGINT NOT NULL,
        post_id BIGINT, comment_id BIGINT,
        listing_id BIGINT,
        reported_user_id BIGINT,
        reason TEXT NOT NULL,
        status TEXT DEFAULT 'open',
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS post_timing(
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS rate_events(
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        action TEXT NOT NULL,
        ip TEXT, content_hash TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS blocked_ips(
        ip TEXT PRIMARY KEY,
        reason TEXT, admin_id BIGINT,
        auto INTEGER DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS ip_events(
        id BIGSERIAL PRIMARY KEY,
        ip TEXT NOT NULL,
        user_id BIGINT,
        action TEXT NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS email_queue(
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT,
        to_email TEXT NOT NULL,
        subject TEXT NOT NULL,
        html TEXT NOT NULL,
        kind TEXT,
        attempts INTEGER DEFAULT 0,
        max_attempts INTEGER DEFAULT 5,
        next_attempt_at TIMESTAMPTZ DEFAULT NOW(),
        status TEXT DEFAULT 'pending',
        last_error TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS verifications(
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        kind TEXT NOT NULL,
        target TEXT NOT NULL,
        code TEXT NOT NULL,
        attempts INTEGER DEFAULT 0,
        max_attempts INTEGER DEFAULT 6,
        verified INTEGER DEFAULT 0,
        expires_at TIMESTAMPTZ NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        verified_at TIMESTAMPTZ
    );
    CREATE TABLE IF NOT EXISTS push_subs(
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        endpoint TEXT UNIQUE NOT NULL,
        p256dh TEXT NOT NULL,
        auth TEXT NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_posts_created ON posts(created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_notif_user ON notifications(user_id, read);
    CREATE INDEX IF NOT EXISTS idx_msgs_conv ON messages(conversation_id, created_at);
    CREATE INDEX IF NOT EXISTS idx_listings_cat ON listings(category, status, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_listings_seller ON listings(seller_id, status);
    CREATE INDEX IF NOT EXISTS idx_offers_listing ON offers(listing_id, status);
    CREATE INDEX IF NOT EXISTS idx_reviews_seller ON seller_reviews(seller_id, created_at DESC);
    """)
    for c, d in [
        ("posts","views_count INTEGER DEFAULT 0"),("posts","hidden INTEGER DEFAULT 0"),
        ("posts","report_count INTEGER DEFAULT 0"),("posts","repost_of BIGINT"),
        ("posts","sound_name TEXT"),("posts","listing_id BIGINT"),
        ("listings","edited_at TIMESTAMPTZ"),("listings","stripe_enabled INTEGER DEFAULT 0"),
        ("listings","condition TEXT DEFAULT 'used'"),
        ("listings","offers_enabled INTEGER DEFAULT 1"),
        ("users","is_admin INTEGER DEFAULT 0"),("users","banned INTEGER DEFAULT 0"),
        ("users","soft_banned INTEGER DEFAULT 0"),("users","strikes INTEGER DEFAULT 0"),
        ("users","trust_score REAL DEFAULT 0"),("users","email_verified INTEGER DEFAULT 0"),
        ("users","phone_verified INTEGER DEFAULT 0"),("users","phone TEXT"),
        ("users","seller_rating REAL DEFAULT 0"),
        ("users","seller_review_count INTEGER DEFAULT 0"),
        ("reports","listing_id BIGINT"),
    ]:
        _add_col(conn, c, d.split()[0], " ".join(d.split()[1:]))
    conn.commit()
    try:
        conn.execute("UPDATE users SET email_verified=1 WHERE email_verified=0 AND created_at < NOW() - INTERVAL '1 minute'")
        conn.execute("DELETE FROM rate_events WHERE created_at < NOW() - INTERVAL '2 days'")
        conn.commit()
    except Exception as e:
        print(f"[init cleanup] {e}")
        conn.rollback()
    conn.close()

# ================= HASH / TOKEN =================
def hash_password(pw):
    s = os.urandom(16)
    h = hashlib.pbkdf2_hmac("sha256", pw.encode(), s, 100_000)
    return s.hex() + "$" + h.hex()

def verify_password(pw, stored):
    s, h = stored.split("$")
    return hashlib.pbkdf2_hmac("sha256", pw.encode(), bytes.fromhex(s), 100_000).hex() == h

def make_token(uid, username):
    return jwt.encode({"sub": str(uid), "username": username,
                       "exp": datetime.utcnow() + timedelta(days=30)},
                      SECRET, algorithm=ALGO)

def current_user(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing token")
    try:
        p = jwt.decode(authorization.split(" ", 1)[1], SECRET, algorithms=[ALGO])
    except jwt.PyJWTError:
        raise HTTPException(401, "Invalid token")
    return {"id": int(p["sub"]), "username": p["username"]}

def optional_user(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        return None
    try:
        p = jwt.decode(authorization.split(" ", 1)[1], SECRET, algorithms=[ALGO])
        return {"id": int(p["sub"]), "username": p["username"]}
    except jwt.PyJWTError:
        return None

def _client_ip(request=None, headers=None):
    if headers:
        for k in ("x-forwarded-for", "X-Forwarded-For"):
            v = headers.get(k)
            if v: return v.split(",")[0].strip()
        for k in ("x-real-ip", "X-Real-IP"):
            v = headers.get(k)
            if v: return v.strip()
    if request and getattr(request, "client", None):
        return request.client.host
    return "unknown"

# ================= WS =================
class WSManager:
    def __init__(self): self.by_user = {}
    async def connect(self, uid, ws):
        await ws.accept()
        self.by_user.setdefault(uid, set()).add(ws)
    def disconnect(self, uid, ws):
        if uid in self.by_user:
            self.by_user[uid].discard(ws)
            if not self.by_user[uid]: del self.by_user[uid]
    async def send_to(self, uid, payload):
        dead = []
        for ws in list(self.by_user.get(uid, [])):
            try: await ws.send_json(payload)
            except: dead.append(ws)
        for ws in dead: self.disconnect(uid, ws)
    async def broadcast(self, uids, payload):
        for uid in set(uids): await self.send_to(uid, payload)
ws_manager = WSManager()

# ================= RATE =================
RateRule = namedtuple("RateRule", ["action", "max", "window_seconds"])
RATE_RULES = {
    "post": RateRule("post", 30, 3600),
    "comment": RateRule("comment", 90, 3600),
    "message": RateRule("message", 300, 3600),
    "listing": RateRule("listing", 20, 86400),
    "follow": RateRule("follow", 200, 3600),
    "offer": RateRule("offer", 50, 3600),
    "review": RateRule("review", 20, 86400),
}

class RateLimited(HTTPException):
    def __init__(self, retry_after):
        super().__init__(429, f"Too many requests. Try again in {retry_after}s.")
        self.headers = {"Retry-After": str(retry_after)}

def check_rate(conn, user_id, action, ip=None, content_hash=None):
    rule = RATE_RULES.get(action)
    if not rule: return
    row = conn.execute("""SELECT COUNT(*) c FROM rate_events WHERE user_id=%s AND action=%s
                          AND created_at > NOW() - INTERVAL '1 second' * %s""",
                       (user_id, action, rule.window_seconds)).fetchone()
    if row and row["c"] >= rule.max: raise RateLimited(rule.window_seconds)
    conn.execute("INSERT INTO rate_events (user_id, action, ip, content_hash) VALUES (?,?,?,?)",
                 (user_id, action, ip, content_hash))

# ================= HELPERS =================
def _blocked_ids(conn, uid):
    rows = conn.execute("""SELECT blocked_id FROM blocks WHERE blocker_id=?
                           UNION SELECT blocker_id FROM blocks WHERE blocked_id=?""", (uid, uid)).fetchall()
    return {r["blocked_id"] for r in rows}

def _is_admin(conn, uid):
    r = conn.execute("SELECT is_admin FROM users WHERE id=?", (uid,)).fetchone()
    return bool(r and r["is_admin"])

def require_admin(u=Depends(current_user)):
    conn = connect()
    ok = _is_admin(conn, u["id"]); conn.close()
    if not ok: raise HTTPException(403, "Admins only")
    return u

def require_not_softbanned(conn, uid):
    row = conn.execute("SELECT soft_banned, banned FROM users WHERE id=?", (uid,)).fetchone()
    if not row: return
    if row["banned"]: raise HTTPException(403, "Account suspended.")
    if row["soft_banned"]: raise HTTPException(403, "Account restricted.")

def push_notification(conn, user_id, actor_id, ntype, post_id=None, comment_id=None):
    if user_id == actor_id: return
    conn.execute("""INSERT INTO notifications (user_id, actor_id, type, post_id, comment_id)
                    VALUES (?,?,?,?,?)""", (user_id, actor_id, ntype, post_id, comment_id))
    try:
        asyncio.get_running_loop().create_task(ws_manager.send_to(user_id, {
            "type": "notification",
            "notification": {"type": ntype, "actor_id": actor_id, "post_id": post_id}}))
    except: pass

def _new_session(conn, user_id, request=None, ua=""):
    sid = secrets.token_urlsafe(24)
    ip = _client_ip(request=request, headers=dict(request.headers)) if request else "unknown"
    conn.execute("INSERT INTO sessions (id, user_id, device, ip, user_agent) VALUES (?,?,?,?,?)",
                 (sid, user_id, "web", ip, (ua or "")[:300]))
    conn.commit()
    return sid

def _gen_code():
    return "".join(secrets.choice("0123456789") for _ in range(6))

def _update_seller_rating(conn, seller_id):
    row = conn.execute("""SELECT AVG(rating) avg_r, COUNT(*) c FROM seller_reviews
                          WHERE seller_id=?""", (seller_id,)).fetchone()
    if row:
        conn.execute("UPDATE users SET seller_rating=?, seller_review_count=? WHERE id=?",
                     (round(row["avg_r"] or 0, 2), row["c"] or 0, seller_id))

# ================= HASHTAGS =================
HASHTAG_RE = re.compile(r"#(\w{1,50})")
def extract_hashtags(conn, post_id, text):
    tags = {m.group(1).lower() for m in HASHTAG_RE.finditer(text or "")}
    for tag in tags:
        conn.execute("INSERT INTO hashtags (tag) VALUES (?) ON CONFLICT DO NOTHING", (tag,))
        row = conn.execute("SELECT id FROM hashtags WHERE tag=?", (tag,)).fetchone()
        if row:
            cur = conn.execute("""INSERT INTO post_hashtags (post_id, hashtag_id) VALUES (?,?)
                                  ON CONFLICT DO NOTHING""", (post_id, row["id"]))
            if cur.rowcount:
                conn.execute("UPDATE hashtags SET post_count = post_count + 1 WHERE id=?", (row["id"],))

# ================= EMAIL =================
def send_email(to_email, subject, html, kind, user_id=None):
    try:
        conn = connect()
        conn.execute("""INSERT INTO email_queue (user_id, to_email, subject, html, kind)
                        VALUES (?,?,?,?,?)""", (user_id, to_email, subject, html, kind))
        conn.commit(); conn.close()
    except: pass

# ================= MODELS =================
class RegisterIn(BaseModel):
    username: str; email: str; password: str; age: int | None = None
class LoginIn(BaseModel):
    email: str; password: str
class PostIn(BaseModel):
    content: str = ""; media_url: str | None = None; media_type: str | None = None
    sound_name: str | None = None; listing_id: int | None = None
class CommentIn(BaseModel):
    body: str; parent_id: int | None = None
class ProfileIn(BaseModel): bio: str | None = None; avatar_url: str | None = None
class DetailsIn(BaseModel):
    age: int | None = None; gender: str | None = None
    country: str | None = None; city: str | None = None
class ViewIn(BaseModel): watch_seconds: float = 0; completed: bool = False
class MessageIn(BaseModel): body: str
class StoryIn(BaseModel): media_url: str; media_type: str; caption: str = ""
class ListingIn(BaseModel):
    title: str; description: str = ""; price: float; currency: str = "USD"
    category: str = "other"; condition: str = "used"
    media_url: str | None = None; media_type: str | None = None
    location: str = ""; offers_enabled: bool = True
class ListingEditIn(BaseModel):
    title: str | None = None; description: str | None = None; price: float | None = None
    category: str | None = None; location: str | None = None
    condition: str | None = None; offers_enabled: bool | None = None
    media_url: str | None = None; media_type: str | None = None
class OfferIn(BaseModel): amount: float; message: str = ""
class OfferRespondIn(BaseModel):
    action: str; counter_amount: float | None = None; message: str = ""
class ReviewIn(BaseModel): rating: int; comment: str = ""; listing_id: int | None = None
class SaveIn(BaseModel): item_type: str; item_id: int
class ReportIn(BaseModel):
    reason: str; post_id: int | None = None
    comment_id: int | None = None; listing_id: int | None = None
    reported_user_id: int | None = None
class DeviceIn(BaseModel):
    fingerprint: str; screen: str = ""; timezone: str = ""; language: str = ""
class ListingImagesIn(BaseModel): images: list[dict]

# ================= STARTUP =================
@app.on_event("startup")
async def _startup():
    if not PG_URL:
        print("⚠️  SUPABASE_PG_URL not set")
        return
    try:
        init_db()
        print("✅ Database schema ready")
    except Exception as e:
        print(f"❌ init_db failed: {e}")

# ================= STATIC =================
def _serve(filename, media_type=None):
    if not os.path.exists(filename):
        raise HTTPException(404, f"{filename} not found")
    if media_type:
        return FileResponse(filename, media_type=media_type)
    return FileResponse(filename)

@app.get("/tourryl.html")
def _ui(): return _serve("tourryl.html", "text/html")
@app.get("/sw.js")
def _sw(): return _serve("sw.js", "application/javascript")
@app.get("/manifest.webmanifest")
def _mf(): return _serve("manifest.webmanifest", "application/manifest+json")
@app.get("/icon-192.png")
def _icon192(): return _serve("icon-192.png", "image/png")
@app.get("/icon-512.png")
def _icon512(): return _serve("icon-512.png", "image/png")
@app.get("/favicon.ico")
def _favicon(): return Response(status_code=204)
@app.get("/")
def _root(): return RedirectResponse("/tourryl.html")
@app.get("/health")
def _health():
    return {"status": "ok", "service": "TouRryl", "db": bool(PG_URL), "cwd": os.getcwd()}

# ================= AUTH =================
@app.post("/auth/register")
def register(d: RegisterIn, request: Request, ua: str = Header(None)):
    if len(d.username) < 3: raise HTTPException(400, "Username 3+ chars")
    if len(d.password) < 6: raise HTTPException(400, "Password 6+ chars")
    if d.age is not None and (d.age < 13 or d.age > 120): raise HTTPException(400, "Age 13-120")
    conn = connect()
    try:
        try:
            cur = conn.execute("INSERT INTO users (username, email, password_hash) VALUES (?,?,?)",
                               (d.username.strip(), d.email.strip().lower(), hash_password(d.password)))
            uid = cur.lastrowid
        except Exception:
            conn.rollback(); conn.close()
            raise HTTPException(400, "Username or email taken")
        if uid == 1: conn.execute("UPDATE users SET is_admin=1 WHERE id=1")
        conn.execute("INSERT INTO user_details (user_id, age) VALUES (?,?) ON CONFLICT DO NOTHING", (uid, d.age))
        conn.execute("INSERT INTO settings (user_id) VALUES (?) ON CONFLICT DO NOTHING", (uid,))
        conn.commit()
        sid = _new_session(conn, uid, request=request, ua=(ua or "")[:300])
        return {"token": make_token(uid, d.username), "session_id": sid,
                "user": {"id": uid, "username": d.username}}
    finally: conn.close()

@app.post("/auth/login")
def login(d: LoginIn, request: Request, ua: str = Header(None)):
    conn = connect()
    r = conn.execute("SELECT * FROM users WHERE email=?", (d.email.strip().lower(),)).fetchone()
    if not r or not verify_password(d.password, r["password_hash"]):
        conn.close(); raise HTTPException(401, "Wrong email or password")
    if r["banned"]: conn.close(); raise HTTPException(403, "Account suspended")
    sid = _new_session(conn, r["id"], request=request, ua=(ua or "")[:300])
    conn.commit(); conn.close()
    return {"token": make_token(r["id"], r["username"]), "session_id": sid,
            "user": {"id": r["id"], "username": r["username"]}}

@app.get("/me")
def me(u=Depends(current_user)):
    conn = connect()
    r = conn.execute("""SELECT id, username, email, bio, avatar_url, followers_count,
                        following_count, is_admin, email_verified, phone_verified, phone,
                        strikes, soft_banned, trust_score, seller_rating, seller_review_count,
                        created_at FROM users WHERE id=?""", (u["id"],)).fetchone()
    conn.close()
    return dict(r)

@app.patch("/me")
def update_me(d: ProfileIn, u=Depends(current_user)):
    conn = connect()
    if d.bio is not None: conn.execute("UPDATE users SET bio=? WHERE id=?", (d.bio.strip()[:200], u["id"]))
    if d.avatar_url is not None: conn.execute("UPDATE users SET avatar_url=? WHERE id=?", (d.avatar_url, u["id"]))
    conn.commit()
    r = conn.execute("SELECT id, username, bio, avatar_url FROM users WHERE id=?", (u["id"],)).fetchone()
    conn.close()
    return dict(r)

@app.get("/me/details")
def get_details(u=Depends(current_user)):
    conn = connect()
    r = conn.execute("""SELECT ud.age, ud.gender, ud.country, ud.city,
                        us.avatar_url, us.bio, us.username, us.email
                        FROM users us LEFT JOIN user_details ud ON ud.user_id=us.id
                        WHERE us.id=?""", (u["id"],)).fetchone()
    conn.close()
    return dict(r) if r else {}

@app.patch("/me/details")
def update_details(d: DetailsIn, u=Depends(current_user)):
    conn = connect()
    if d.age is not None and (d.age < 13 or d.age > 120):
        conn.close(); raise HTTPException(400, "Age 13-120")
    conn.execute("INSERT INTO user_details (user_id) VALUES (?) ON CONFLICT DO NOTHING", (u["id"],))
    if d.age is not None: conn.execute("UPDATE user_details SET age=? WHERE user_id=?", (d.age, u["id"]))
    if d.gender is not None: conn.execute("UPDATE user_details SET gender=? WHERE user_id=?", (d.gender[:20], u["id"]))
    if d.country is not None: conn.execute("UPDATE user_details SET country=? WHERE user_id=?", (d.country[:60], u["id"]))
    if d.city is not None: conn.execute("UPDATE user_details SET city=? WHERE user_id=?", (d.city[:60], u["id"]))
    conn.commit(); conn.close()
    return {"ok": True}

@app.post("/me/avatar")
async def upload_avatar(file: UploadFile = File(...), u=Depends(current_user)):
    ctype = file.content_type or ""
    ext = os.path.splitext(file.filename or "")[1].lower()
    if not ctype.startswith("image/") and ext not in (".jpg",".jpeg",".png",".webp"):
        raise HTTPException(400, "Image only")
    if not ext: ext = ".jpg"
    name = f"avatar_{uuid.uuid4().hex}{ext}"
    path = os.path.join(UPLOAD_DIR, name)
    with open(path, "wb") as out:
        while chunk := await file.read(1024*1024): out.write(chunk)
    url = f"/uploads/{name}"
    conn = connect()
    conn.execute("UPDATE users SET avatar_url=? WHERE id=?", (url, u["id"]))
    conn.commit(); conn.close()
    return {"avatar_url": url}

@app.post("/upload")
async def upload(file: UploadFile = File(...), u=Depends(current_user)):
    ctype = file.content_type or ""
    ext = os.path.splitext(file.filename or "")[1].lower()
    if ctype.startswith("video/") or ext in (".mp4",".webm",".mov",".m4v"): mt = "video"
    elif ctype.startswith("image/") or ext in (".jpg",".jpeg",".png",".gif",".webp"): mt = "image"
    else: raise HTTPException(400, "Only images/videos")
    if not ext: ext = ".mp4" if mt == "video" else ".jpg"
    name = f"{uuid.uuid4().hex}{ext}"
    path = os.path.join(UPLOAD_DIR, name)
    size, limit = 0, MAX_UPLOAD_MB*1024*1024
    with open(path, "wb") as out:
        while chunk := await file.read(1024*1024):
            size += len(chunk)
            if size > limit:
                out.close(); os.remove(path); raise HTTPException(413, f"Max {MAX_UPLOAD_MB}MB")
            out.write(chunk)
    return {"url": f"/uploads/{name}", "type": mt}

# ================= POSTS / FEEDS =================
def _attach_listing(conn, d):
    if not d.get("listing_id"): return d
    try:
        l = conn.execute("""SELECT id, title, price, currency, media_url, status
                            FROM listings WHERE id=?""", (d["listing_id"],)).fetchone()
        if l: d["linked_listing"] = dict(l)
    except: pass
    return d

def _enrich_post(conn, d, u):
    d.setdefault("liked", False); d.setdefault("saved", False)
    d.setdefault("following_author", False); d.setdefault("reposted", False)
    if u:
        try: d["liked"] = bool(conn.execute("SELECT 1 FROM likes WHERE post_id=? AND user_id=?",
                                             (d["id"], u["id"])).fetchone())
        except: pass
        try: d["saved"] = bool(conn.execute("SELECT 1 FROM saves WHERE user_id=? AND item_type='post' AND item_id=?",
                                             (u["id"], d["id"])).fetchone())
        except: pass
        if "author_id" in d:
            try: d["following_author"] = bool(conn.execute("""SELECT 1 FROM follows
                                                               WHERE follower_id=? AND following_id=?""",
                                                           (u["id"], d["author_id"])).fetchone())
            except: pass
    d = _attach_listing(conn, d)
    return d

@app.post("/posts")
def create_post(p: PostIn, u=Depends(current_user)):
    if not p.content.strip() and not p.media_url: raise HTTPException(400, "Post needs text or media")
    conn = connect()
    require_not_softbanned(conn, u["id"])
    cur = conn.execute("""INSERT INTO posts (author_id, content, media_url, media_type, sound_name, listing_id)
                          VALUES (?,?,?,?,?,?)""",
                       (u["id"], p.content.strip(), p.media_url, p.media_type,
                        (p.sound_name or "").strip()[:120] or None, p.listing_id))
    pid = cur.lastrowid
    extract_hashtags(conn, pid, p.content)
    conn.commit(); conn.close()
    return {"id": pid, "ok": True}

@app.post("/posts/{pid}/repost")
def repost(pid: int, u=Depends(current_user)):
    conn = connect()
    orig = conn.execute("SELECT * FROM posts WHERE id=? AND hidden=0", (pid,)).fetchone()
    if not orig: conn.close(); raise HTTPException(404, "Not found")
    if orig["repost_of"]: conn.close(); raise HTTPException(400, "Can't repost a repost")
    existing = conn.execute("SELECT id FROM posts WHERE author_id=? AND repost_of=?",
                            (u["id"], pid)).fetchone()
    if existing:
        conn.execute("DELETE FROM posts WHERE id=?", (existing["id"],))
        conn.commit(); conn.close()
        return {"reposted": False}
    cur = conn.execute("""INSERT INTO posts (author_id, content, media_url, media_type, repost_of)
                          VALUES (?,?,?,?,?)""", (u["id"], "", None, None, pid))
    new_id = cur.lastrowid
    push_notification(conn, orig["author_id"], u["id"], "repost", post_id=pid)
    conn.commit(); conn.close()
    return {"reposted": True, "post_id": new_id}

@app.delete("/posts/{pid}")
def delete_post(pid: int, u=Depends(current_user)):
    conn = connect()
    r = conn.execute("SELECT author_id FROM posts WHERE id=?", (pid,)).fetchone()
    if not r: conn.close(); raise HTTPException(404, "Not found")
    if r["author_id"] != u["id"] and not _is_admin(conn, u["id"]):
        conn.close(); raise HTTPException(403, "Not yours")
    conn.execute("DELETE FROM posts WHERE id=?", (pid,))
    conn.commit(); conn.close()
    return {"ok": True}

def _feed_query(conn, where_extra="", params_extra=(), limit=20, offset=0):
    sql = f"""
        SELECT p.id, p.content, p.media_url, p.media_type, p.likes_count, p.comments_count,
               p.views_count, p.created_at, p.sound_name, p.listing_id,
               u.id as author_id, u.username, u.avatar_url
        FROM posts p
        JOIN users u ON u.id = p.author_id
        WHERE p.hidden = 0 AND u.banned = 0 AND u.soft_banned = 0
        {where_extra}
        ORDER BY p.created_at DESC
        LIMIT ? OFFSET ?
    """
    return conn.execute(sql, tuple(params_extra) + (limit, offset)).fetchall()

@app.get("/feed/foryou")
def feed_foryou(limit: int = 20, offset: int = 0, u=Depends(optional_user)):
    try:
        conn = connect()
        rows = _feed_query(conn, limit=min(limit, 100), offset=offset)
        now = datetime.utcnow()
        scored = []
        for r in rows:
            d = dict(r)
            try:
                created = d["created_at"]
                if isinstance(created, str):
                    created = datetime.fromisoformat(created.replace("Z", "+00:00"))
                hours = max(0.1, (now - created.replace(tzinfo=None)).total_seconds() / 3600)
            except: hours = 1.0
            raw = (d.get("likes_count") or 0)*3 + (d.get("comments_count") or 0)*5 + (d.get("views_count") or 0)*0.5
            d["score"] = raw / ((hours + 2.0) ** 1.4)
            scored.append(d)
        scored.sort(key=lambda x: x["score"], reverse=True)
        page = scored[:limit]
        blocked = _blocked_ids(conn, u["id"]) if u else set()
        out = []
        for d in page:
            if d["author_id"] in blocked: continue
            d.pop("score", None)
            out.append(_enrich_post(conn, d, u))
        conn.close()
        return out
    except Exception as e:
        import traceback; traceback.print_exc()
        return []

@app.get("/feed/videos")
def feed_videos(limit: int = 20, offset: int = 0, u=Depends(optional_user)):
    try:
        conn = connect()
        rows = _feed_query(conn, where_extra="AND p.media_type = 'video'",
                           limit=min(limit, 100), offset=offset)
        now = datetime.utcnow()
        scored = []
        for r in rows:
            d = dict(r)
            try:
                created = d["created_at"]
                if isinstance(created, str):
                    created = datetime.fromisoformat(created.replace("Z", "+00:00"))
                hours = max(0.1, (now - created.replace(tzinfo=None)).total_seconds() / 3600)
            except: hours = 1.0
            raw = (d.get("likes_count") or 0)*3 + (d.get("comments_count") or 0)*5 + (d.get("views_count") or 0)*1
            d["score"] = raw / ((hours + 2.0) ** 1.3)
            scored.append(d)
        scored.sort(key=lambda x: x["score"], reverse=True)
        page = scored[:limit]
        blocked = _blocked_ids(conn, u["id"]) if u else set()
        out = []
        for d in page:
            if d["author_id"] in blocked: continue
            d.pop("score", None)
            out.append(_enrich_post(conn, d, u))
        conn.close()
        return out
    except Exception as e:
        import traceback; traceback.print_exc()
        return []

@app.get("/feed/following")
def feed_following(limit: int = 20, offset: int = 0, u=Depends(current_user)):
    try:
        conn = connect()
        rows = _feed_query(conn, where_extra="AND p.author_id IN (SELECT following_id FROM follows WHERE follower_id=?)",
                           params_extra=(u["id"],), limit=min(limit, 100), offset=offset)
        blocked = _blocked_ids(conn, u["id"])
        out = [_enrich_post(conn, dict(r), u) for r in rows if r["author_id"] not in blocked]
        conn.close()
        return out
    except: return []

@app.post("/posts/{pid}/like")
def like_post(pid: int, u=Depends(current_user)):
    conn = connect()
    post = conn.execute("SELECT author_id FROM posts WHERE id=?", (pid,)).fetchone()
    if not post: conn.close(); raise HTTPException(404, "Not found")
    existing = conn.execute("SELECT 1 FROM likes WHERE post_id=? AND user_id=?", (pid, u["id"])).fetchone()
    if existing:
        conn.execute("DELETE FROM likes WHERE post_id=? AND user_id=?", (pid, u["id"]))
        conn.execute("UPDATE posts SET likes_count=GREATEST(0,likes_count-1) WHERE id=?", (pid,))
        conn.commit(); conn.close(); return {"liked": False}
    conn.execute("INSERT INTO likes (post_id, user_id) VALUES (?,?)", (pid, u["id"]))
    conn.execute("UPDATE posts SET likes_count=likes_count+1 WHERE id=?", (pid,))
    push_notification(conn, post["author_id"], u["id"], "like", post_id=pid)
    conn.commit(); conn.close()
    return {"liked": True}

@app.post("/posts/{pid}/view")
def record_view(pid: int, v: ViewIn, u=Depends(optional_user)):
    try:
        conn = connect()
        conn.execute("INSERT INTO post_views (post_id, user_id, watch_seconds, completed) VALUES (?,?,?,?)",
                     (pid, u["id"] if u else None, v.watch_seconds, 1 if v.completed else 0))
        conn.execute("UPDATE posts SET views_count = views_count + 1 WHERE id=?", (pid,))
        conn.commit(); conn.close()
        return {"ok": True}
    except: return {"ok": False}

@app.get("/posts/{pid}/comments")
def get_comments(pid: int, u=Depends(optional_user)):
    conn = connect()
    rows = conn.execute("""SELECT c.*, u.username, u.avatar_url FROM comments c
                           JOIN users u ON u.id=c.user_id
                           WHERE c.post_id=? ORDER BY c.created_at ASC""", (pid,)).fetchall()
    out = []
    for r in rows:
        d = dict(r); d["liked"] = False
        if u:
            try: d["liked"] = bool(conn.execute("SELECT 1 FROM comment_likes WHERE comment_id=? AND user_id=?",
                                                 (d["id"], u["id"])).fetchone())
            except: pass
        out.append(d)
    conn.close()
    return out

@app.post("/posts/{pid}/comments")
def add_comment(pid: int, d: CommentIn, u=Depends(current_user)):
    body = d.body.strip()
    if not body or len(body) > 500: raise HTTPException(400, "Comment 1-500 chars")
    conn = connect()
    post = conn.execute("SELECT author_id FROM posts WHERE id=?", (pid,)).fetchone()
    if not post: conn.close(); raise HTTPException(404, "Post not found")
    cur = conn.execute("INSERT INTO comments (post_id, user_id, parent_id, body) VALUES (?,?,?,?)",
                       (pid, u["id"], d.parent_id, body))
    conn.execute("UPDATE posts SET comments_count=comments_count+1 WHERE id=?", (pid,))
    cid = cur.lastrowid
    if post["author_id"] != u["id"]:
        push_notification(conn, post["author_id"], u["id"], "comment", post_id=pid, comment_id=cid)
    conn.commit()
    r = conn.execute("""SELECT c.*, u.username, u.avatar_url FROM comments c
                        JOIN users u ON u.id=c.user_id WHERE c.id=?""", (cid,)).fetchone()
    conn.close()
    return dict(r)

@app.post("/comments/{cid}/like")
def like_comment(cid: int, u=Depends(current_user)):
    conn = connect()
    c = conn.execute("SELECT user_id FROM comments WHERE id=?", (cid,)).fetchone()
    if not c: conn.close(); raise HTTPException(404, "Not found")
    ex = conn.execute("SELECT 1 FROM comment_likes WHERE comment_id=? AND user_id=?", (cid, u["id"])).fetchone()
    if ex:
        conn.execute("DELETE FROM comment_likes WHERE comment_id=? AND user_id=?", (cid, u["id"]))
        conn.execute("UPDATE comments SET likes_count=GREATEST(0,likes_count-1) WHERE id=?", (cid,))
        conn.commit(); conn.close(); return {"liked": False}
    conn.execute("INSERT INTO comment_likes (comment_id, user_id) VALUES (?,?)", (cid, u["id"]))
    conn.execute("UPDATE comments SET likes_count=likes_count+1 WHERE id=?", (cid,))
    push_notification(conn, c["user_id"], u["id"], "comment_like")
    conn.commit(); conn.close()
    return {"liked": True}

# ================= FOLLOWS / USERS =================
@app.post("/users/{uid}/follow")
def follow_user(uid: int, u=Depends(current_user)):
    if uid == u["id"]: raise HTTPException(400, "Can't follow yourself")
    conn = connect()
    if not conn.execute("SELECT 1 FROM users WHERE id=?", (uid,)).fetchone():
        conn.close(); raise HTTPException(404, "User not found")
    ex = conn.execute("SELECT 1 FROM follows WHERE follower_id=? AND following_id=?", (u["id"], uid)).fetchone()
    if ex:
        conn.execute("DELETE FROM follows WHERE follower_id=? AND following_id=?", (u["id"], uid))
        conn.execute("UPDATE users SET followers_count=GREATEST(0,followers_count-1) WHERE id=?", (uid,))
        conn.execute("UPDATE users SET following_count=GREATEST(0,following_count-1) WHERE id=?", (u["id"],))
        conn.commit(); conn.close(); return {"following": False}
    conn.execute("INSERT INTO follows (follower_id, following_id) VALUES (?,?)", (u["id"], uid))
    conn.execute("UPDATE users SET followers_count=followers_count+1 WHERE id=?", (uid,))
    conn.execute("UPDATE users SET following_count=following_count+1 WHERE id=?", (u["id"],))
    push_notification(conn, uid, u["id"], "follow")
    conn.commit(); conn.close()
    return {"following": True}

@app.get("/users/{uid}")
def get_user(uid: int, u=Depends(optional_user)):
    conn = connect()
    r = conn.execute("""SELECT id, username, bio, avatar_url, followers_count,
                        following_count, seller_rating, seller_review_count, created_at
                        FROM users WHERE id=?""", (uid,)).fetchone()
    if not r: conn.close(); raise HTTPException(404, "Not found")
    d = dict(r)
    d["is_me"] = bool(u and u["id"] == uid)
    d["following"] = bool(u and conn.execute("SELECT 1 FROM follows WHERE follower_id=? AND following_id=?",
                                              (u["id"], uid)).fetchone())
    conn.close()
    return d

@app.get("/users/{uid}/posts")
def user_posts(uid: int, limit: int = 30, offset: int = 0, u=Depends(optional_user)):
    try:
        conn = connect()
        rows = _feed_query(conn, where_extra="AND p.author_id = ?",
                           params_extra=(uid,), limit=min(limit, 100), offset=offset)
        out = [_enrich_post(conn, dict(r), u) for r in rows]
        conn.close()
        return out
    except: return []

# ================= SAVES =================
@app.post("/saves/toggle")
def toggle_save(d: SaveIn, u=Depends(current_user)):
    if d.item_type not in ("post","listing"): raise HTTPException(400, "Bad item_type")
    conn = connect()
    ex = conn.execute("SELECT 1 FROM saves WHERE user_id=? AND item_type=? AND item_id=?",
                      (u["id"], d.item_type, d.item_id)).fetchone()
    if ex:
        conn.execute("DELETE FROM saves WHERE user_id=? AND item_type=? AND item_id=?",
                     (u["id"], d.item_type, d.item_id))
        conn.commit(); conn.close(); return {"saved": False}
    conn.execute("INSERT INTO saves (user_id, item_type, item_id) VALUES (?,?,?)",
                 (u["id"], d.item_type, d.item_id))
    conn.commit(); conn.close()
    return {"saved": True}

@app.get("/saves")
def get_saves(item_type: str = None, u=Depends(current_user)):
    conn = connect()
    sql = "SELECT item_type, item_id FROM saves WHERE user_id=?"
    params = [u["id"]]
    if item_type: sql += " AND item_type=?"; params.append(item_type)
    sql += " ORDER BY created_at DESC LIMIT 200"
    rows = conn.execute(sql, params).fetchall()
    out = {"posts": [], "listings": []}
    for r in rows:
        if r["item_type"] == "post":
            p = conn.execute("""SELECT p.*, u.username, u.avatar_url FROM posts p
                                JOIN users u ON u.id=p.author_id
                                WHERE p.id=? AND p.hidden=0""", (r["item_id"],)).fetchone()
            if p:
                d = _enrich_post(conn, dict(p), u); d["saved"] = True
                out["posts"].append(d)
        else:
            l = conn.execute("""SELECT l.*, u.username, u.avatar_url FROM listings l
                                JOIN users u ON u.id=l.seller_id WHERE l.id=?""",
                             (r["item_id"],)).fetchone()
            if l:
                d = _enrich_listing(conn, dict(l), u); d["saved"] = True
                out["listings"].append(d)
    conn.close()
    return out

# ================= LISTINGS =================
def _enrich_listing(conn, d, u=None):
    try:
        imgs = conn.execute("""SELECT media_url, media_type FROM listing_images
                               WHERE listing_id=? ORDER BY position""", (d["id"],)).fetchall()
        d["images"] = [dict(i) for i in imgs]
    except: d["images"] = []
    if not d.get("media_url") and d["images"]:
        d["media_url"] = d["images"][0]["media_url"]
        d["media_type"] = d["images"][0]["media_type"]
    d["saved"] = False
    if u:
        try: d["saved"] = bool(conn.execute("SELECT 1 FROM saves WHERE user_id=? AND item_type='listing' AND item_id=?",
                                             (u["id"], d["id"])).fetchone())
        except: pass
    return d

@app.post("/listings")
def create_listing(d: ListingIn, u=Depends(current_user)):
    if not d.title.strip(): raise HTTPException(400, "Title required")
    if d.price < 0: raise HTTPException(400, "Bad price")
    if d.condition not in ("new","used","refurbished"): d.condition = "used"
    conn = connect()
    cur = conn.execute("""INSERT INTO listings (seller_id, title, description, price, currency,
                          category, media_url, media_type, location, condition, offers_enabled)
                          VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                       (u["id"], d.title.strip()[:120], d.description.strip()[:1000], d.price,
                        d.currency[:6], d.category[:30], d.media_url, d.media_type,
                        d.location.strip()[:120], d.condition, 1 if d.offers_enabled else 0))
    conn.commit()
    lid = cur.lastrowid; conn.close()
    return {"id": lid}

@app.post("/listings/{lid}/images")
def add_listing_images(lid: int, d: ListingImagesIn, u=Depends(current_user)):
    conn = connect()
    l = conn.execute("SELECT seller_id FROM listings WHERE id=?", (lid,)).fetchone()
    if not l: conn.close(); raise HTTPException(404, "Not found")
    if l["seller_id"] != u["id"]: conn.close(); raise HTTPException(403, "Not yours")
    pos = 0
    for img in d.images[:8]:
        url = (img.get("media_url") or "").strip()
        if not url: continue
        conn.execute("""INSERT INTO listing_images (listing_id, media_url, media_type, position)
                        VALUES (?,?,?,?)""", (lid, url, img.get("media_type","image"), pos))
        pos += 1
    cur_l = conn.execute("SELECT media_url FROM listings WHERE id=?", (lid,)).fetchone()
    if cur_l and not cur_l["media_url"]:
        first = conn.execute("""SELECT media_url, media_type FROM listing_images
                                WHERE listing_id=? ORDER BY position LIMIT 1""", (lid,)).fetchone()
        if first:
            conn.execute("UPDATE listings SET media_url=?, media_type=? WHERE id=?",
                         (first["media_url"], first["media_type"], lid))
    conn.commit(); conn.close()
    return {"ok": True, "count": pos}

@app.get("/listings")
def list_listings(category: str = None, q: str = None,
                  min_price: float = None, max_price: float = None,
                  condition: str = None, location: str = None,
                  seller_id: int = None,
                  limit: int = 40, offset: int = 0, u=Depends(optional_user)):
    conn = connect()
    sql = """SELECT l.*, us.username, us.avatar_url, us.seller_rating, us.seller_review_count
             FROM listings l JOIN users us ON us.id=l.seller_id
             WHERE l.status='active' AND us.banned=0 AND us.soft_banned=0"""
    params = []
    if category and category != "all": sql += " AND l.category=%s"; params.append(category)
    if condition: sql += " AND l.condition=%s"; params.append(condition)
    if q:
        sql += " AND (l.title ILIKE %s OR l.description ILIKE %s)"
        like = f"%{q}%"; params += [like, like]
    if min_price is not None: sql += " AND l.price >= %s"; params.append(min_price)
    if max_price is not None: sql += " AND l.price <= %s"; params.append(max_price)
    if location: sql += " AND l.location ILIKE %s"; params.append(f"%{location}%")
    if seller_id: sql += " AND l.seller_id = %s"; params.append(seller_id)
    sql += " ORDER BY l.created_at DESC LIMIT %s OFFSET %s"
    params += [min(limit, 100), offset]
    rows = conn.execute(sql, params).fetchall()
    out = [_enrich_listing(conn, dict(r), u) for r in rows]
    conn.close()
    return out

@app.get("/listings/{lid}")
def get_listing(lid: int, u=Depends(optional_user)):
    conn = connect()
    conn.execute("UPDATE listings SET views_count=views_count+1 WHERE id=?", (lid,))
    conn.commit()
    r = conn.execute("""SELECT l.*, us.username, us.avatar_url, us.seller_rating,
                               us.seller_review_count, us.created_at as seller_since
                        FROM listings l JOIN users us ON us.id=l.seller_id WHERE l.id=?""",
                     (lid,)).fetchone()
    if not r: conn.close(); raise HTTPException(404, "Not found")
    d = _enrich_listing(conn, dict(r), u)
    others = conn.execute("""SELECT id, title, price, currency, media_url, media_type
                             FROM listings WHERE seller_id=? AND id != ? AND status='active'
                             LIMIT 6""", (r["seller_id"], lid)).fetchall()
    d["other_listings"] = [dict(o) for o in others]
    related = conn.execute("""SELECT id, title, price, currency, media_url, media_type
                              FROM listings WHERE category=? AND id != ? AND seller_id != ?
                              AND status='active' LIMIT 6""",
                           (r["category"], lid, r["seller_id"])).fetchall()
    d["related"] = [dict(x) for x in related]
    conn.close()
    return d

@app.post("/listings/{lid}/sold")
def mark_sold(lid: int, u=Depends(current_user)):
    conn = connect()
    r = conn.execute("SELECT seller_id FROM listings WHERE id=?", (lid,)).fetchone()
    if not r: conn.close(); raise HTTPException(404, "Not found")
    if r["seller_id"] != u["id"] and not _is_admin(conn, u["id"]):
        conn.close(); raise HTTPException(403, "Not yours")
    conn.execute("UPDATE listings SET status='sold' WHERE id=?", (lid,))
    conn.commit(); conn.close()
    return {"ok": True}

@app.delete("/listings/{lid}")
def delete_listing(lid: int, u=Depends(current_user)):
    conn = connect()
    r = conn.execute("SELECT seller_id FROM listings WHERE id=?", (lid,)).fetchone()
    if not r: conn.close(); raise HTTPException(404, "Not found")
    if r["seller_id"] != u["id"] and not _is_admin(conn, u["id"]):
        conn.close(); raise HTTPException(403, "Not yours")
    conn.execute("DELETE FROM listings WHERE id=?", (lid,))
    conn.commit(); conn.close()
    return {"ok": True}

# ================= OFFERS =================
@app.post("/listings/{lid}/offers")
def make_offer(lid: int, d: OfferIn, u=Depends(current_user)):
    if d.amount <= 0: raise HTTPException(400, "Bad amount")
    conn = connect()
    l = conn.execute("SELECT seller_id, offers_enabled, status, currency FROM listings WHERE id=?", (lid,)).fetchone()
    if not l: conn.close(); raise HTTPException(404, "Not found")
    if l["seller_id"] == u["id"]: conn.close(); raise HTTPException(400, "Can't offer on own listing")
    if not l["offers_enabled"]: conn.close(); raise HTTPException(400, "Offers disabled")
    if l["status"] != "active": conn.close(); raise HTTPException(400, "Not available")
    existing = conn.execute("""SELECT id FROM offers WHERE listing_id=? AND buyer_id=? AND status='pending'""",
                            (lid, u["id"])).fetchone()
    if existing:
        conn.execute("""UPDATE offers SET amount=?, message=?, created_at=NOW() WHERE id=?""",
                     (d.amount, d.message.strip()[:300], existing["id"]))
    else:
        conn.execute("""INSERT INTO offers (listing_id, buyer_id, amount, currency, message)
                        VALUES (?,?,?,?,?)""",
                     (lid, u["id"], d.amount, l["currency"] or "USD", d.message.strip()[:300]))
    push_notification(conn, l["seller_id"], u["id"], "offer_made", post_id=lid)
    conn.commit(); conn.close()
    return {"ok": True}

@app.get("/listings/{lid}/offers")
def listing_offers(lid: int, u=Depends(current_user)):
    conn = connect()
    l = conn.execute("SELECT seller_id FROM listings WHERE id=?", (lid,)).fetchone()
    if not l: conn.close(); raise HTTPException(404, "Not found")
    if l["seller_id"] != u["id"]: conn.close(); raise HTTPException(403, "Not yours")
    rows = conn.execute("""SELECT o.*, us.username, us.avatar_url FROM offers o
                           JOIN users us ON us.id=o.buyer_id
                           WHERE o.listing_id=? ORDER BY o.created_at DESC""", (lid,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.get("/offers/mine")
def my_offers(u=Depends(current_user)):
    conn = connect()
    rows = conn.execute("""SELECT o.*, l.title as listing_title, l.media_url, l.media_type,
                                  l.seller_id, us.username as seller_username
                           FROM offers o JOIN listings l ON l.id=o.listing_id
                           JOIN users us ON us.id=l.seller_id
                           WHERE o.buyer_id=? ORDER BY o.created_at DESC LIMIT 50""",
                        (u["id"],)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.post("/offers/{oid}/respond")
def respond_offer(oid: int, d: OfferRespondIn, u=Depends(current_user)):
    if d.action not in ("accept","decline","counter"): raise HTTPException(400, "Bad action")
    conn = connect()
    o = conn.execute("""SELECT o.*, l.seller_id FROM offers o
                        JOIN listings l ON l.id=o.listing_id WHERE o.id=?""", (oid,)).fetchone()
    if not o: conn.close(); raise HTTPException(404, "Not found")
    if o["seller_id"] != u["id"]: conn.close(); raise HTTPException(403, "Not yours")
    if o["status"] not in ("pending","countered"):
        conn.close(); raise HTTPException(400, "Already responded")
    if d.action == "accept":
        conn.execute("UPDATE offers SET status='accepted', responded_at=NOW() WHERE id=?", (oid,))
        conn.execute("""UPDATE offers SET status='declined', responded_at=NOW()
                        WHERE listing_id=? AND id != ? AND status='pending'""", (o["listing_id"], oid))
        push_notification(conn, o["buyer_id"], u["id"], "offer_accepted", post_id=o["listing_id"])
    elif d.action == "decline":
        conn.execute("UPDATE offers SET status='declined', responded_at=NOW() WHERE id=?", (oid,))
        push_notification(conn, o["buyer_id"], u["id"], "offer_declined", post_id=o["listing_id"])
    else:
        if not d.counter_amount or d.counter_amount <= 0:
            conn.close(); raise HTTPException(400, "Counter needs amount")
        conn.execute("""UPDATE offers SET status='countered', counter_amount=?,
                        message=?, responded_at=NOW() WHERE id=?""",
                     (d.counter_amount, d.message.strip()[:300], oid))
        push_notification(conn, o["buyer_id"], u["id"], "offer_countered", post_id=o["listing_id"])
    conn.commit(); conn.close()
    return {"ok": True, "action": d.action}

# ================= REVIEWS =================
@app.post("/users/{uid}/reviews")
def create_review(uid: int, d: ReviewIn, u=Depends(current_user)):
    if uid == u["id"]: raise HTTPException(400, "Can't review yourself")
    if d.rating < 1 or d.rating > 5: raise HTTPException(400, "Rating 1-5")
    conn = connect()
    if not conn.execute("SELECT 1 FROM users WHERE id=?", (uid,)).fetchone():
        conn.close(); raise HTTPException(404, "User not found")
    existing = conn.execute("""SELECT id FROM seller_reviews WHERE seller_id=? AND buyer_id=?
                               AND COALESCE(listing_id, 0) = COALESCE(?, 0)""",
                            (uid, u["id"], d.listing_id)).fetchone()
    if existing:
        conn.execute("UPDATE seller_reviews SET rating=?, comment=?, created_at=NOW() WHERE id=?",
                     (d.rating, d.comment.strip()[:500], existing["id"]))
    else:
        conn.execute("""INSERT INTO seller_reviews (seller_id, buyer_id, listing_id, rating, comment)
                        VALUES (?,?,?,?,?)""",
                     (uid, u["id"], d.listing_id, d.rating, d.comment.strip()[:500]))
    _update_seller_rating(conn, uid)
    push_notification(conn, uid, u["id"], "review_received")
    conn.commit(); conn.close()
    return {"ok": True}

@app.get("/users/{uid}/reviews")
def get_reviews(uid: int, limit: int = 20):
    conn = connect()
    rows = conn.execute("""SELECT r.*, u.username, u.avatar_url
                           FROM seller_reviews r JOIN users u ON u.id=r.buyer_id
                           WHERE r.seller_id=? ORDER BY r.created_at DESC LIMIT ?""",
                        (uid, min(limit, 50))).fetchall()
    summary = conn.execute("""SELECT AVG(rating) avg_r, COUNT(*) c FROM seller_reviews
                              WHERE seller_id=?""", (uid,)).fetchone()
    conn.close()
    return {"reviews": [dict(r) for r in rows],
            "average": round(summary["avg_r"] or 0, 2),
            "count": summary["c"] or 0}

# ================= STORIES =================
@app.post("/stories")
def create_story(s: StoryIn, u=Depends(current_user)):
    if s.media_type not in ("image","video"): raise HTTPException(400, "Bad media type")
    conn = connect()
    conn.execute("""INSERT INTO stories (user_id, media_url, media_type, caption, expires_at)
                    VALUES (?,?,?,?, NOW() + INTERVAL '24 hours')""",
                 (u["id"], s.media_url, s.media_type, s.caption.strip()[:200]))
    conn.commit(); conn.close()
    return {"ok": True}

@app.get("/stories")
def list_stories(u=Depends(optional_user)):
    conn = connect()
    blocked = _blocked_ids(conn, u["id"]) if u else set()
    rows = conn.execute("""SELECT s.*, us.username, us.avatar_url
                           FROM stories s JOIN users us ON us.id=s.user_id
                           WHERE s.expires_at > NOW()
                           ORDER BY s.created_at DESC""").fetchall()
    conn.close()
    grouped = {}
    for r in rows:
        if r["user_id"] in blocked: continue
        d = dict(r)
        grouped.setdefault(r["user_id"], {"user": {"id": r["user_id"], "username": r["username"],
                                                   "avatar_url": r["avatar_url"]}, "stories": []})
        grouped[r["user_id"]]["stories"].append(d)
    return list(grouped.values())

@app.post("/stories/{sid}/view")
def view_story(sid: int, u=Depends(current_user)):
    conn = connect()
    try:
        ex = conn.execute("SELECT 1 FROM story_views WHERE story_id=? AND viewer_id=?", (sid, u["id"])).fetchone()
        if not ex:
            conn.execute("INSERT INTO story_views (story_id, viewer_id) VALUES (?,?)", (sid, u["id"]))
            conn.execute("UPDATE stories SET views_count=views_count+1 WHERE id=?", (sid,))
            conn.commit()
    except: pass
    finally: conn.close()
    return {"ok": True}

# ================= HASHTAGS / SEARCH =================
@app.get("/hashtags/trending")
def trending_tags(limit: int = 20):
    conn = connect()
    rows = conn.execute("""
        SELECT h.tag, h.post_count,
               (SELECT COUNT(*) FROM post_hashtags ph JOIN posts p ON p.id=ph.post_id
                WHERE ph.hashtag_id=h.id AND p.created_at > NOW() - INTERVAL '24 hours') AS recent
        FROM hashtags h WHERE h.post_count > 0
        ORDER BY recent DESC, post_count DESC LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.get("/hashtags/{tag}")
def hashtag_feed(tag: str, u=Depends(optional_user)):
    conn = connect()
    rows = conn.execute("""
        SELECT p.*, u.username, u.avatar_url FROM posts p
        JOIN post_hashtags ph ON ph.post_id=p.id
        JOIN hashtags h ON h.id=ph.hashtag_id
        JOIN users u ON u.id=p.author_id
        WHERE h.tag=? AND p.hidden=0 AND u.banned=0 AND u.soft_banned=0
        ORDER BY p.created_at DESC LIMIT 50
    """, (tag.lower(),)).fetchall()
    blocked = _blocked_ids(conn, u["id"]) if u else set()
    out = [_enrich_post(conn, dict(r), u) for r in rows if r["author_id"] not in blocked]
    conn.close()
    return out

@app.get("/search")
def search(q: str = Query(..., min_length=1), u=Depends(optional_user)):
    like = f"%{q.strip()}%"
    tag = q.strip().lstrip("#").lower()
    conn = connect()
    blocked = _blocked_ids(conn, u["id"]) if u else set()
    users = [dict(r) for r in conn.execute(
        "SELECT id, username, avatar_url FROM users WHERE banned=0 AND username ILIKE ? LIMIT 10",
        (like,)).fetchall() if r["id"] not in blocked]
    posts = [dict(r) for r in conn.execute("""
        SELECT p.*, u.username, u.avatar_url FROM posts p
        JOIN users u ON u.id=p.author_id
        WHERE p.hidden=0 AND u.banned=0 AND p.content ILIKE ?
        ORDER BY p.created_at DESC LIMIT 20
    """, (like,)).fetchall() if r["author_id"] not in blocked]
    posts = [_enrich_post(conn, p, u) for p in posts]
    hashtags = [dict(r) for r in conn.execute(
        "SELECT tag, post_count FROM hashtags WHERE tag ILIKE ? ORDER BY post_count DESC LIMIT 10",
        (f"%{tag}%",)).fetchall()]
    listings = [dict(r) for r in conn.execute("""
        SELECT l.*, u.username, u.avatar_url FROM listings l
        JOIN users u ON u.id=l.seller_id
        WHERE l.status='active' AND (l.title ILIKE ? OR l.description ILIKE ?)
        LIMIT 10
    """, (like, like)).fetchall()]
    if u:
        try:
            conn.execute("INSERT INTO search_history (user_id, query) VALUES (?,?)",
                         (u["id"], q.strip()[:100]))
            conn.commit()
        except: pass
    conn.close()
    return {"users": users, "posts": posts, "hashtags": hashtags, "listings": listings}

# ================= MESSAGES =================
def _in_conv(conn, cid, uid):
    r = conn.execute("SELECT user_a, user_b FROM conversations WHERE id=?", (cid,)).fetchone()
    return r if r and uid in (r["user_a"], r["user_b"]) else None

@app.get("/conversations")
def conversations(u=Depends(current_user)):
    conn = connect()
    rows = conn.execute("""
        SELECT c.id, c.last_message_at,
               CASE WHEN c.user_a=? THEN c.user_b ELSE c.user_a END AS other_id,
               ou.username AS other_username, ou.avatar_url AS other_avatar,
               (SELECT body FROM messages WHERE conversation_id=c.id ORDER BY created_at DESC LIMIT 1) AS last_body,
               (SELECT COUNT(*) FROM messages WHERE conversation_id=c.id AND sender_id != ? AND read=0) AS unread
        FROM conversations c
        JOIN users ou ON ou.id=(CASE WHEN c.user_a=? THEN c.user_b ELSE c.user_a END)
        WHERE c.user_a=? OR c.user_b=?
        ORDER BY c.last_message_at DESC
    """, (u["id"], u["id"], u["id"], u["id"], u["id"])).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.post("/conversations/with/{other_id}")
def start_conversation(other_id: int, u=Depends(current_user)):
    if other_id == u["id"]: raise HTTPException(400, "Can't message yourself")
    a, b = sorted([u["id"], other_id])
    conn = connect()
    if not conn.execute("SELECT 1 FROM users WHERE id=?", (other_id,)).fetchone():
        conn.close(); raise HTTPException(404, "User not found")
    row = conn.execute("SELECT id FROM conversations WHERE user_a=? AND user_b=?", (a, b)).fetchone()
    if row: conn.close(); return {"id": row["id"]}
    cur = conn.execute("INSERT INTO conversations (user_a, user_b) VALUES (?,?)", (a, b))
    conn.commit(); cid = cur.lastrowid; conn.close()
    return {"id": cid}

@app.get("/conversations/{cid}/messages")
def get_messages(cid: int, since: int = 0, u=Depends(current_user)):
    conn = connect()
    if not _in_conv(conn, cid, u["id"]): conn.close(); raise HTTPException(404, "Not found")
    conn.execute("UPDATE messages SET read=1 WHERE conversation_id=? AND sender_id != ?", (cid, u["id"]))
    conn.commit()
    rows = conn.execute("""SELECT m.id, m.sender_id, m.body, m.created_at, s.username as sender_username
                           FROM messages m JOIN users s ON s.id=m.sender_id
                           WHERE m.conversation_id=? AND m.id > ?
                           ORDER BY m.created_at ASC LIMIT 200""", (cid, since)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.post("/conversations/{cid}/messages")
def send_message(cid: int, m: MessageIn, u=Depends(current_user)):
    text = m.body.strip()
    if not text or len(text) > 2000: raise HTTPException(400, "1-2000 chars")
    conn = connect()
    conv = _in_conv(conn, cid, u["id"])
    if not conv: conn.close(); raise HTTPException(404, "Not found")
    cur = conn.execute("INSERT INTO messages (conversation_id, sender_id, body) VALUES (?,?,?)",
                       (cid, u["id"], text))
    conn.execute("UPDATE conversations SET last_message_at=NOW() WHERE id=?", (cid,))
    other = conv["user_b"] if u["id"] == conv["user_a"] else conv["user_a"]
    push_notification(conn, other, u["id"], "message")
    conn.commit()
    mid = cur.lastrowid
    row = conn.execute("""SELECT m.id, m.sender_id, m.body, m.created_at, s.username as sender_username
                          FROM messages m JOIN users s ON s.id=m.sender_id WHERE m.id=?""", (mid,)).fetchone()
    conn.close()
    try:
        asyncio.get_running_loop().create_task(ws_manager.broadcast([u["id"], other], {
            "type": "message", "conversation_id": cid, "message": dict(row)}))
    except: pass
    return dict(row)

# ================= NOTIFICATIONS =================
@app.get("/notifications")
def notifications(limit: int = 50, u=Depends(current_user)):
    conn = connect()
    rows = conn.execute("""SELECT n.id, n.type, n.post_id, n.comment_id, n.read, n.created_at,
                                  a.id as actor_id, a.username as actor_username,
                                  a.avatar_url as actor_avatar
                           FROM notifications n JOIN users a ON a.id=n.actor_id
                           WHERE n.user_id=? ORDER BY n.created_at DESC LIMIT ?""",
                        (u["id"], limit)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

@app.post("/notifications/read")
def notif_read(u=Depends(current_user)):
    conn = connect()
    conn.execute("UPDATE notifications SET read=1 WHERE user_id=?", (u["id"],))
    conn.commit(); conn.close()
    return {"ok": True}

@app.get("/badges")
def badges(u=Depends(current_user)):
    conn = connect()
    n = conn.execute("SELECT COUNT(*) c FROM notifications WHERE user_id=? AND read=0", (u["id"],)).fetchone()["c"]
    m = conn.execute("""SELECT COUNT(*) c FROM messages m JOIN conversations c ON c.id=m.conversation_id
                        WHERE (c.user_a=? OR c.user_b=?) AND m.sender_id != ? AND m.read=0""",
                     (u["id"], u["id"], u["id"])).fetchone()["c"]
    conn.close()
    return {"notifications": n, "messages": m}

# ================= WEBSOCKET =================
def _token_uid(token):
    try: return int(jwt.decode(token, SECRET, algorithms=[ALGO])["sub"])
    except: return None

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket, token: str = Query(...)):
    uid = _token_uid(token)
    if not uid: await ws.close(code=4401); return
    await ws_manager.connect(uid, ws)
    try:
        while True:
            data = await ws.receive_json()
            if data.get("type") == "typing":
                to = data.get("to"); cid = data.get("conversation_id")
                if to: await ws_manager.send_to(to, {"type": "typing", "from": uid, "conversation_id": cid})
            elif data.get("type") == "ping":
                await ws.send_json({"type": "pong"})
    except WebSocketDisconnect: ws_manager.disconnect(uid, ws)
    except: ws_manager.disconnect(uid, ws)

# ================= REPORTS / BLOCKS =================
@app.post("/reports")
def create_report(d: ReportIn, u=Depends(current_user)):
    if not any([d.post_id, d.comment_id, d.listing_id, d.reported_user_id]):
        raise HTTPException(400, "Nothing to report")
    conn = connect()
    conn.execute("""INSERT INTO reports (reporter_id, post_id, comment_id, listing_id,
                    reported_user_id, reason) VALUES (?,?,?,?,?,?)""",
                 (u["id"], d.post_id, d.comment_id, d.listing_id, d.reported_user_id, d.reason.strip()))
    conn.commit(); conn.close()
    return {"ok": True}

@app.post("/users/{uid}/block")
def toggle_block(uid: int, u=Depends(current_user)):
    if uid == u["id"]: raise HTTPException(400, "Can't block yourself")
    conn = connect()
    ex = conn.execute("SELECT 1 FROM blocks WHERE blocker_id=? AND blocked_id=?", (u["id"], uid)).fetchone()
    if ex:
        conn.execute("DELETE FROM blocks WHERE blocker_id=? AND blocked_id=?", (u["id"], uid))
        conn.commit(); conn.close(); return {"blocked": False}
    conn.execute("INSERT INTO blocks (blocker_id, blocked_id) VALUES (?,?)", (u["id"], uid))
    conn.commit(); conn.close()
    return {"blocked": True}

@app.get("/users/{uid}/blocked")
def is_blocked(uid: int, u=Depends(current_user)):
    conn = connect()
    b = bool(conn.execute("SELECT 1 FROM blocks WHERE blocker_id=? AND blocked_id=?",
                          (u["id"], uid)).fetchone())
    conn.close()
    return {"blocked": b}

# ================= SETTINGS =================
@app.get("/settings")
def get_settings(u=Depends(current_user)):
    conn = connect()
    r = conn.execute("SELECT * FROM settings WHERE user_id=?", (u["id"],)).fetchone()
    if not r:
        conn.execute("INSERT INTO settings (user_id) VALUES (?) ON CONFLICT DO NOTHING", (u["id"],))
        conn.commit()
        r = conn.execute("SELECT * FROM settings WHERE user_id=?", (u["id"],)).fetchone()
    conn.close()
    return dict(r)

@app.patch("/settings")
def update_settings(d: dict, u=Depends(current_user)):
    conn = connect()
    conn.execute("INSERT INTO settings (user_id) VALUES (?) ON CONFLICT DO NOTHING", (u["id"],))
    if "data_saver" in d: conn.execute("UPDATE settings SET data_saver=? WHERE user_id=?", (1 if d["data_saver"] else 0, u["id"]))
    if "autoplay_video" in d: conn.execute("UPDATE settings SET autoplay_video=? WHERE user_id=?", (1 if d["autoplay_video"] else 0, u["id"]))
    conn.commit(); conn.close()
    return {"ok": True}

@app.get("/privacy")
def privacy():
    return HTMLResponse("""<!doctype html><html><head><meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1"><title>Privacy</title>
    <style>body{background:#000;color:#fff;font-family:-apple-system,sans-serif;max-width:720px;margin:0 auto;padding:24px;line-height:1.6}h1{color:#00E58A}</style></head><body>
    <h1>TouRryl Privacy Policy</h1>
    <p>We collect: email, username, hashed password, optional profile picture, age, IP, device fingerprint, and content you post.</p>
    <p>We do not sell your personal info.</p>
    </body></html>""")

@app.delete("/me")
def delete_account(confirm: str = "", u=Depends(current_user)):
    if confirm != "DELETE": raise HTTPException(400, "Add ?confirm=DELETE")
    conn = connect()
    uid = u["id"]
    conn.execute("UPDATE messages SET body='[deleted]' WHERE sender_id=?", (uid,))
    conn.execute("DELETE FROM users WHERE id=?", (uid,))
    conn.commit(); conn.close()
    return {"deleted": True}

# ================= DEVICES / ADMIN =================
@app.post("/devices/register")
def register_device(d: DeviceIn, request: Request, u=Depends(current_user)):
    fp = d.fingerprint.strip()[:200]
    if not fp or len(fp) < 8: raise HTTPException(400, "Bad fingerprint")
    ip = _client_ip(request=request, headers=dict(request.headers))
    conn = connect()
    try:
        conn.execute("""INSERT INTO devices (user_id, fingerprint, screen, timezone, language, ip)
                        VALUES (?,?,?,?,?,?)
                        ON CONFLICT (user_id, fingerprint) DO UPDATE SET
                        last_seen=NOW(), ip=EXCLUDED.ip""",
                     (u["id"], fp, d.screen[:40], d.timezone[:60], d.language[:20], ip))
        conn.commit()
    except: pass
    conn.close()
    return {"ok": True}

@app.get("/admin/stats")
def admin_stats(u=Depends(require_admin)):
    conn = connect()
    def one(q): return conn.execute(q).fetchone()["c"]
    s = {"users": one("SELECT COUNT(*) c FROM users"),
         "posts": one("SELECT COUNT(*) c FROM posts WHERE hidden=0"),
         "listings": one("SELECT COUNT(*) c FROM listings WHERE status='active'"),
         "pending_offers": one("SELECT COUNT(*) c FROM offers WHERE status='pending'"),
         "open_reports": one("SELECT COUNT(*) c FROM reports WHERE status='open'"),
         "banned": one("SELECT COUNT(*) c FROM users WHERE banned=1")}
    conn.close()
    return s

@app.get("/admin/users")
def admin_users(q: str = None, u=Depends(require_admin)):
    conn = connect()
    sql = "SELECT id, username, email, banned, soft_banned, strikes, is_admin, created_at FROM users"
    params = []
    if q:
        sql += " WHERE username ILIKE ? OR email ILIKE ?"
        params += [f"%{q}%", f"%{q}%"]
    sql += " ORDER BY created_at DESC LIMIT 100"
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]

# ================= RUN =================
if __name__ == "__main__":
    print("=" * 60)
    print("  TouRryl API  →  http://0.0.0.0:8000")
    print(f"  Database:    {'Supabase' if PG_URL else 'NOT CONFIGURED'}")
    print("=" * 60)
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")