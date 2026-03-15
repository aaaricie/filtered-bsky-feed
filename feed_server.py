"""
Multi-User Bluesky Feed Generator Server

Connects to the Bluesky Jetstream firehose, buffers posts for 5 minutes,
re-fetches engagement (T5), scores them, and serves per-user personalized
feeds via the Bluesky feed generator protocol.

Each user's feed is filtered to posts from accounts they follow.
No OAuth or user login required — the requesting user's DID is extracted
from the JWT that Bluesky's PDS sends automatically.

Usage:
    pip install websockets zstandard atproto
    python feed_server.py
"""

import urllib.request
import urllib.parse
import json
import datetime
import math
import os
import sys
import time
import threading
import sqlite3
import asyncio
from collections import OrderedDict
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn

# Optional imports — checked at startup
try:
    import websockets
except ImportError:
    websockets = None

try:
    import zstandard as zstd
except ImportError:
    zstd = None

try:
    from atproto_server.auth.jwt import verify_jwt
    from atproto import IdResolver
    HAS_ATPROTO = True
except ImportError:
    HAS_ATPROTO = False

# ─────────────────────────────────────────────
#  CONFIGURATION (override via environment)
# ─────────────────────────────────────────────

BSKY_HANDLE      = os.environ.get("BSKY_HANDLE", "parisien.cc")
BSKY_APP_PASSWORD = os.environ.get("BSKY_APP_PASSWORD", "")
BSKY_API         = "https://bsky.social/xrpc"
PUBLIC_API       = "https://public.api.bsky.app/xrpc"

FEED_HOSTNAME = os.environ.get("FEED_HOSTNAME", "feed.parisien.cc")
FEED_PORT     = int(os.environ.get("FEED_PORT", "8000"))
FEED_NAME     = os.environ.get("FEED_NAME", "filtered-following")

DELAY_SECONDS     = int(os.environ.get("DELAY_SECONDS", "300"))
REFETCH_BATCH     = 25
SCORE_FLOOR       = 17
PRUNE_HOURS       = int(os.environ.get("PRUNE_HOURS", "48"))
FOLLOWS_CACHE_TTL = int(os.environ.get("FOLLOWS_CACHE_TTL", "3600"))  # 1 hour
REPOST_HALF_LIFE  = float(os.environ.get("REPOST_HALF_LIFE", "6.0"))  # hours

JETSTREAM_URL = os.environ.get(
    "JETSTREAM_URL",
    "wss://jetstream2.us-east.bsky.network/subscribe"
)
JETSTREAM_MAX_DIDS = 10_000  # per connection

DB_PATH     = os.path.join(os.path.dirname(os.path.abspath(__file__)), "feed.db")
CURSOR_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jetstream_cursor.txt")
ZSTD_DICT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "zstd_dictionary")

UTC = datetime.timezone.utc


# ─────────────────────────────────────────────
#  HTTP HELPERS
# ─────────────────────────────────────────────

def _request(url, *, token=None, payload=None):
    data = json.dumps(payload).encode() if payload else None
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=headers,
                                method="POST" if data else "GET")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())


# ─────────────────────────────────────────────
#  BLUESKY AUTH (for getPosts API calls)
# ─────────────────────────────────────────────

def authenticate(handle, app_password):
    print(f"[auth] Authenticating as {handle} ...", flush=True)
    resp = _request(
        f"{BSKY_API}/com.atproto.server.createSession",
        payload={"identifier": handle, "password": app_password}
    )
    return resp["accessJwt"], resp["did"]


# ─────────────────────────────────────────────
#  FOLLOWS RESOLUTION & CACHE
# ─────────────────────────────────────────────

def fetch_user_follows(user_did):
    """Fetch all accounts a user follows via public API. No auth needed."""
    follows = []
    cursor = None
    while True:
        url = (f"{PUBLIC_API}/app.bsky.graph.getFollows"
               f"?actor={urllib.parse.quote(user_did)}&limit=100")
        if cursor:
            url += f"&cursor={urllib.parse.quote(cursor)}"
        resp = _request(url)
        for f in resp.get("follows", []):
            did = f.get("did")
            if did:
                follows.append(did)
        cursor = resp.get("cursor")
        if not cursor:
            break
    return follows


def get_cached_follows(conn, user_did):
    """Return cached follows if fresh enough, else None."""
    cutoff = (datetime.datetime.now(UTC) - datetime.timedelta(seconds=FOLLOWS_CACHE_TTL)).isoformat()
    rows = conn.execute(
        "SELECT follows_did FROM follows_cache WHERE user_did = ? AND cached_at > ?",
        (user_did, cutoff)
    ).fetchall()
    if rows:
        return [r[0] for r in rows]
    return None


def store_follows_cache(conn, user_did, follows_dids):
    """Store a user's follows in the cache."""
    now = datetime.datetime.now(UTC).isoformat()
    conn.execute("DELETE FROM follows_cache WHERE user_did = ?", (user_did,))
    conn.executemany(
        "INSERT INTO follows_cache (user_did, follows_did, cached_at) VALUES (?, ?, ?)",
        [(user_did, did, now) for did in follows_dids]
    )
    conn.commit()


def get_or_fetch_follows(conn, user_did, interesting_dids_lock, interesting_dids, ws_update_fn):
    """Get follows from cache or fetch fresh. Updates interesting_dids if new DIDs found."""
    cached = get_cached_follows(conn, user_did)
    if cached is not None:
        return cached

    print(f"[follows] Fetching follows for {user_did} ...", flush=True)
    try:
        follows = fetch_user_follows(user_did)
    except Exception as e:
        print(f"[follows] Failed to fetch for {user_did}: {e}", flush=True)
        # Try stale cache
        rows = conn.execute(
            "SELECT follows_did FROM follows_cache WHERE user_did = ?",
            (user_did,)
        ).fetchall()
        if rows:
            print(f"[follows] Using stale cache ({len(rows)} follows)", flush=True)
            return [r[0] for r in rows]
        return []

    store_follows_cache(conn, user_did, follows)
    print(f"[follows] Cached {len(follows)} follows for {user_did}", flush=True)

    # Update interesting_dids and notify Jetstream consumer
    new_dids = set()
    with interesting_dids_lock:
        for did in follows:
            if did not in interesting_dids:
                new_dids.add(did)
                interesting_dids.add(did)

    if new_dids and ws_update_fn:
        print(f"[follows] {len(new_dids)} new DIDs added to Jetstream filter", flush=True)
        ws_update_fn()

    return follows


# ─────────────────────────────────────────────
#  ENGAGEMENT RE-FETCH
# ─────────────────────────────────────────────

def refetch_engagement(token, uris):
    """Batch-fetch T5 engagement via getPosts API."""
    results = {}
    for i in range(0, len(uris), REFETCH_BATCH):
        batch = uris[i:i + REFETCH_BATCH]
        params = "&".join(f"uris={urllib.parse.quote(u)}" for u in batch)
        url = f"{BSKY_API}/app.bsky.feed.getPosts?{params}"
        try:
            resp = _request(url, token=token)
            for p in resp.get("posts", []):
                uri = p.get("uri", "")
                embed = p.get("embed", {})
                embed_type = embed.get("$type", "") if embed else ""
                results[uri] = {
                    "likeCount":   p.get("likeCount", 0) or 0,
                    "repostCount": p.get("repostCount", 0) or 0,
                    "replyCount":  p.get("replyCount", 0) or 0,
                    "quoteCount":  p.get("quoteCount", 0) or 0,
                    "createdAt":   p.get("record", {}).get("createdAt", ""),
                    "has_images":  "images" in embed_type,
                    "has_link":    "external" in embed_type,
                }
        except Exception as e:
            print(f"[refetch] Warning: getPosts batch failed: {e}", flush=True)
    return results


# ─────────────────────────────────────────────
#  JETSTREAM EVENT PARSING
# ─────────────────────────────────────────────

def parse_post_event(event):
    """Parse a Jetstream commit event for app.bsky.feed.post into a buffer record."""
    commit = event.get("commit", {})
    record = commit.get("record", {})
    did = event.get("did", "")
    rkey = commit.get("rkey", "")
    cid = commit.get("cid", "")

    uri = f"at://{did}/app.bsky.feed.post/{rkey}"

    reply = record.get("reply")
    is_reply = reply is not None
    is_self_reply = False
    if is_reply:
        parent_uri = reply.get("parent", {}).get("uri", "")
        # Self-reply if parent post is by the same author
        is_self_reply = parent_uri.startswith(f"at://{did}/")

    embed = record.get("embed", {})
    embed_type = embed.get("$type", "") if embed else ""
    has_images = "images" in embed_type
    has_link = "external" in embed_type

    created_at = record.get("createdAt", "")

    return {
        "type": "post",
        "uri": uri,
        "cid": cid,
        "author_did": did,
        "created_at": created_at,
        "is_reply": is_reply,
        "is_self_reply": is_self_reply,
        "has_images": has_images,
        "has_link": has_link,
    }


def parse_repost_event(event):
    """Parse a Jetstream commit event for app.bsky.feed.repost into a buffer record."""
    commit = event.get("commit", {})
    record = commit.get("record", {})
    did = event.get("did", "")
    rkey = commit.get("rkey", "")
    cid = commit.get("cid", "")

    subject = record.get("subject", {})
    subject_uri = subject.get("uri", "")
    repost_uri = f"at://{did}/app.bsky.feed.repost/{rkey}"
    created_at = record.get("createdAt", "")

    return {
        "type": "repost",
        "reposter_did": did,
        "subject_uri": subject_uri,
        "repost_uri": repost_uri,
        "repost_cid": cid,
        "created_at": created_at,
        "original_created_at": None,  # filled during T5 re-fetch
    }


# ─────────────────────────────────────────────
#  SCORING
# ─────────────────────────────────────────────

def score_post(rec, t5):
    """Score a post. Returns the integer score."""
    t5_likes   = t5.get("likeCount", 0)
    t5_reposts = t5.get("repostCount", 0)
    t5_replies = t5.get("replyCount", 0)

    # Raw engagement (subject to decay for reposts)
    raw = 0
    raw += min(t5_likes, 150)
    raw += t5_reposts * 3
    raw += t5_replies * 2
    raw += t5_likes * 2  # velocity

    # Repost age decay: only applied to raw engagement
    is_repost = rec.get("type") == "repost"
    if is_repost and rec.get("original_created_at"):
        try:
            orig_dt = datetime.datetime.fromisoformat(
                rec["original_created_at"].replace("Z", "+00:00")
            )
            age_hours = (datetime.datetime.now(UTC) - orig_dt).total_seconds() / 3600
            if age_hours > 0:
                raw = round(raw * (0.5 ** (age_hours / REPOST_HALF_LIFE)))
        except Exception:
            pass

    score = raw

    # Bonuses (NOT decayed)
    if rec.get("has_link") and t5_likes >= 1:
        score += 15
    if rec.get("has_images") and t5_likes >= 1:
        score += 10

    # Penalties (NOT decayed)
    if is_repost:
        score -= 15
    if rec.get("is_reply") and not rec.get("is_self_reply"):
        score -= 25
    if rec.get("is_self_reply"):
        score -= 5

    return score


# ─────────────────────────────────────────────
#  DATABASE
# ─────────────────────────────────────────────

_DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS posts (
    uri         TEXT PRIMARY KEY,
    cid         TEXT NOT NULL,
    author_did  TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    indexed_at  TEXT NOT NULL,
    is_reply    INTEGER DEFAULT 0,
    is_self_reply INTEGER DEFAULT 0,
    has_images  INTEGER DEFAULT 0,
    has_link    INTEGER DEFAULT 0,
    score       INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS reposts (
    id                  TEXT PRIMARY KEY,
    reposter_did        TEXT NOT NULL,
    subject_uri         TEXT NOT NULL,
    repost_uri          TEXT NOT NULL,
    created_at          TEXT NOT NULL,
    original_created_at TEXT,
    indexed_at          TEXT NOT NULL,
    score               INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS follows_cache (
    user_did    TEXT NOT NULL,
    follows_did TEXT NOT NULL,
    cached_at   TEXT NOT NULL,
    PRIMARY KEY (user_did, follows_did)
);
"""

_DB_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_posts_author ON posts(author_did);
CREATE INDEX IF NOT EXISTS idx_posts_indexed ON posts(indexed_at);
CREATE INDEX IF NOT EXISTS idx_reposts_reposter ON reposts(reposter_did);
CREATE INDEX IF NOT EXISTS idx_reposts_indexed ON reposts(indexed_at);
CREATE INDEX IF NOT EXISTS idx_follows_user ON follows_cache(user_did);
"""


def _configure_conn(conn):
    """Enable WAL mode and busy timeout for concurrent access."""
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")


def init_db(conn):
    _configure_conn(conn)
    conn.executescript(_DB_SCHEMA)
    conn.executescript(_DB_INDEXES)
    conn.commit()


def store_post_db(conn, rec, score):
    now = datetime.datetime.now(UTC).isoformat()
    conn.execute(
        """INSERT OR REPLACE INTO posts
           (uri, cid, author_did, created_at, indexed_at,
            is_reply, is_self_reply, has_images, has_link, score)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (rec["uri"], rec["cid"], rec["author_did"], rec["created_at"], now,
         int(rec["is_reply"]), int(rec["is_self_reply"]),
         int(rec["has_images"]), int(rec["has_link"]), score)
    )


def store_repost_db(conn, rec, score):
    now = datetime.datetime.now(UTC).isoformat()
    entry_id = f"{rec['reposter_did']}::{rec['subject_uri']}"
    conn.execute(
        """INSERT OR REPLACE INTO reposts
           (id, reposter_did, subject_uri, repost_uri,
            created_at, original_created_at, indexed_at, score)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (entry_id, rec["reposter_did"], rec["subject_uri"], rec["repost_uri"],
         rec["created_at"], rec.get("original_created_at"), now, score)
    )


def prune_old(conn, hours):
    cutoff = (datetime.datetime.now(UTC) - datetime.timedelta(hours=hours)).isoformat()
    conn.execute("DELETE FROM posts WHERE indexed_at < ?", (cutoff,))
    conn.execute("DELETE FROM reposts WHERE indexed_at < ?", (cutoff,))
    conn.commit()


def prune_stale_follows(conn, max_age_hours=72):
    """Remove follows cache entries older than max_age_hours."""
    cutoff = (datetime.datetime.now(UTC) - datetime.timedelta(hours=max_age_hours)).isoformat()
    conn.execute("DELETE FROM follows_cache WHERE cached_at < ?", (cutoff,))
    conn.commit()


def get_user_feed(conn, followed_dids, cursor_ts=None, limit=50):
    """Query posts + reposts for a specific user's followed accounts.

    Uses a temp table for the IN clause to avoid SQLite variable limits.
    Returns list of (uri, indexed_at, repost_uri_or_none) sorted by indexed_at DESC.
    """
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS _follows (did TEXT PRIMARY KEY)")
    conn.execute("DELETE FROM _follows")
    conn.executemany("INSERT OR IGNORE INTO _follows VALUES (?)",
                     [(d,) for d in followed_dids])

    # Deduplicate: if the same post_uri appears as both a direct post and
    # a repost, keep only the most recent entry. ROW_NUMBER ensures the
    # correct repost_uri is returned for the winning row.
    if cursor_ts:
        rows = conn.execute("""
            WITH combined AS (
                SELECT uri as post_uri, indexed_at, NULL as repost_uri
                FROM posts
                WHERE author_did IN (SELECT did FROM _follows)
                  AND indexed_at < ? AND score >= ?
                UNION ALL
                SELECT subject_uri, indexed_at, repost_uri
                FROM reposts
                WHERE reposter_did IN (SELECT did FROM _follows)
                  AND indexed_at < ? AND score >= ?
            ),
            ranked AS (
                SELECT post_uri, indexed_at, repost_uri,
                       ROW_NUMBER() OVER (PARTITION BY post_uri ORDER BY indexed_at DESC) as rn
                FROM combined
            )
            SELECT post_uri, indexed_at, repost_uri
            FROM ranked WHERE rn = 1
            ORDER BY indexed_at DESC
            LIMIT ?
        """, (cursor_ts, SCORE_FLOOR, cursor_ts, SCORE_FLOOR, limit)).fetchall()
    else:
        rows = conn.execute("""
            WITH combined AS (
                SELECT uri as post_uri, indexed_at, NULL as repost_uri
                FROM posts
                WHERE author_did IN (SELECT did FROM _follows)
                  AND score >= ?
                UNION ALL
                SELECT subject_uri, indexed_at, repost_uri
                FROM reposts
                WHERE reposter_did IN (SELECT did FROM _follows)
                  AND score >= ?
            ),
            ranked AS (
                SELECT post_uri, indexed_at, repost_uri,
                       ROW_NUMBER() OVER (PARTITION BY post_uri ORDER BY indexed_at DESC) as rn
                FROM combined
            )
            SELECT post_uri, indexed_at, repost_uri
            FROM ranked WHERE rn = 1
            ORDER BY indexed_at DESC
            LIMIT ?
        """, (SCORE_FLOOR, SCORE_FLOOR, limit)).fetchall()

    return rows


# ─────────────────────────────────────────────
#  JWT VERIFICATION
# ─────────────────────────────────────────────

_id_resolver = None
_signing_key_cache = {}
_signing_key_lock = threading.Lock()


def _init_jwt():
    global _id_resolver
    if HAS_ATPROTO:
        _id_resolver = IdResolver()


def _get_signing_key(did, force_refresh):
    """Callback for verify_jwt — resolves a DID to its atproto signing key."""
    with _signing_key_lock:
        if not force_refresh and did in _signing_key_cache:
            return _signing_key_cache[did]
    key = _id_resolver.did.resolve_atproto_key(did, force_refresh)
    with _signing_key_lock:
        _signing_key_cache[did] = key
    return key


def verify_request_jwt(auth_header):
    """Verify the Authorization header and return the requester's DID.

    Returns None if auth is missing or invalid.
    """
    if not HAS_ATPROTO:
        print("[jwt] WARNING: atproto not installed, skipping JWT verification", flush=True)
        return None

    if not auth_header or not auth_header.startswith("Bearer "):
        return None

    token = auth_header[7:]
    own_did = f"did:web:{FEED_HOSTNAME}"

    try:
        payload = verify_jwt(token, _get_signing_key, own_did)
        return payload.iss
    except Exception as e:
        print(f"[jwt] Verification failed: {e}", flush=True)
        return None


# ─────────────────────────────────────────────
#  CURSOR PERSISTENCE
# ─────────────────────────────────────────────

def save_cursor(time_us):
    try:
        with open(CURSOR_FILE, "w") as f:
            f.write(str(time_us))
    except Exception:
        pass


def load_cursor():
    try:
        with open(CURSOR_FILE, "r") as f:
            return int(f.read().strip())
    except (FileNotFoundError, ValueError):
        return None


# ─────────────────────────────────────────────
#  ZSTD DICTIONARY
# ─────────────────────────────────────────────

def load_zstd_dict():
    """Load the Jetstream zstd dictionary, downloading if needed."""
    if zstd is None:
        return None

    if not os.path.exists(ZSTD_DICT_PATH):
        print("[zstd] Downloading dictionary ...", flush=True)
        dict_url = ("https://raw.githubusercontent.com/bluesky-social/"
                    "jetstream/main/pkg/models/zstd_dictionary")
        try:
            req = urllib.request.Request(dict_url)
            with urllib.request.urlopen(req, timeout=30) as r:
                data = r.read()
            with open(ZSTD_DICT_PATH, "wb") as f:
                f.write(data)
            print(f"[zstd] Dictionary saved ({len(data)} bytes)", flush=True)
        except Exception as e:
            print(f"[zstd] Failed to download dictionary: {e}", flush=True)
            return None

    with open(ZSTD_DICT_PATH, "rb") as f:
        dict_data = zstd.ZstdCompressionDict(f.read())
    return zstd.ZstdDecompressor(dict_data=dict_data)


# ─────────────────────────────────────────────
#  JETSTREAM CONSUMER
# ─────────────────────────────────────────────

class JetstreamConsumer:
    """Connects to Jetstream, buffers posts/reposts, processes T5 engagement."""

    def __init__(self, interesting_dids, interesting_dids_lock):
        self.interesting_dids = interesting_dids
        self.interesting_dids_lock = interesting_dids_lock
        self.buffer = OrderedDict()  # key -> (record, discovered_monotonic)
        self.buffer_lock = threading.Lock()
        self.token = None
        self.publisher_did = None
        self.last_auth = 0
        self.running = False
        self.connected = False
        self.last_cursor = load_cursor()
        self.decompressor = load_zstd_dict()
        self.stats = {
            "events": 0, "buffered": 0, "scored": 0,
            "stored": 0, "dropped": 0, "connections": 0,
        }
        self._ws_connections = []  # list of active WebSocket connections
        self._loop = None
        self._update_event = None
        self._delete_lock = threading.Lock()
        self._pending_deletes = []

    def _authenticate(self):
        self.token, self.publisher_did = authenticate(BSKY_HANDLE, BSKY_APP_PASSWORD)
        self.last_auth = time.monotonic()

    def start(self):
        self.running = True
        thread = threading.Thread(target=self._run_thread, daemon=True)
        thread.start()
        return thread

    def request_dids_update(self):
        """Called from other threads to trigger a wantedDids update."""
        if self._loop and self._update_event:
            self._loop.call_soon_threadsafe(self._update_event.set)

    def _run_thread(self):
        """Entry point for the consumer thread — runs an asyncio event loop."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._update_event = asyncio.Event()
        self._loop.run_until_complete(self._run_async())

    async def _run_async(self):
        self._authenticate()
        backoff = 1

        while self.running:
            try:
                await self._connect_and_consume()
                backoff = 1  # reset on clean disconnect
            except Exception as e:
                print(f"[jetstream] Connection error: {e}", flush=True)
                self.connected = False

            if self.running:
                wait = min(backoff, 60)
                print(f"[jetstream] Reconnecting in {wait}s ...", flush=True)
                await asyncio.sleep(wait)
                backoff = min(backoff * 2, 60)

    def _build_ws_url(self):
        """Build the Jetstream WebSocket URL with collection filters only.

        DIDs are sent via options_update after connecting to avoid URL length limits.
        """
        params = [
            "wantedCollections=app.bsky.feed.post",
            "wantedCollections=app.bsky.feed.repost",
        ]
        if self.decompressor:
            params.append("compress=true")
        if self.last_cursor:
            # Rewind by DELAY_SECONDS + 10s so buffered posts are replayed on restart
            safe_cursor = self.last_cursor - (DELAY_SECONDS + 10) * 1_000_000
            params.append(f"cursor={safe_cursor}")
        return f"{JETSTREAM_URL}?{'&'.join(params)}"

    async def _connect_and_consume(self):
        """Connect to Jetstream and process events."""
        with self.interesting_dids_lock:
            all_dids = list(self.interesting_dids)

        # Don't connect until we have DIDs to filter for
        if not all_dids:
            print("[jetstream] No DIDs to track yet, waiting for first user ...", flush=True)
            # Wait for a DID update signal
            await self._update_event.wait()
            self._update_event.clear()
            with self.interesting_dids_lock:
                all_dids = list(self.interesting_dids)
            if not all_dids:
                return  # still empty, will retry via reconnect loop

        # Shard DIDs across multiple connections if needed
        shards = []
        for i in range(0, len(all_dids), JETSTREAM_MAX_DIDS):
            shards.append(all_dids[i:i + JETSTREAM_MAX_DIDS])

        print(f"[jetstream] Connecting ({len(all_dids)} DIDs, "
              f"{len(shards)} shard(s)) ...", flush=True)

        url = self._build_ws_url()
        tasks = []
        for shard in shards:
            tasks.append(asyncio.create_task(self._consume_shard(url, shard)))

        # Also run the buffer processor and update listener
        tasks.append(asyncio.create_task(self._process_buffer_loop()))
        tasks.append(asyncio.create_task(self._listen_for_updates()))

        self.connected = True
        self.stats["connections"] += 1

        try:
            # Wait until any task fails (triggers reconnect)
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
            for t in done:
                if t.exception():
                    raise t.exception()
        finally:
            self.connected = False
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _consume_shard(self, url, dids_shard=None):
        """Consume events from a single Jetstream WebSocket connection."""
        extra_headers = {}
        if self.decompressor:
            extra_headers["Socket-Encoding"] = "zstd"

        async with websockets.connect(
            url,
            additional_headers=extra_headers,
            ping_interval=30,
            ping_timeout=10,
            max_size=10_000_000,
        ) as ws:
            self._ws_connections.append(ws)

            # Send wantedDids via options_update after connecting
            if dids_shard:
                msg = json.dumps({
                    "type": "options_update",
                    "payload": {
                        "wantedCollections": [
                            "app.bsky.feed.post",
                            "app.bsky.feed.repost",
                        ],
                        "wantedDids": dids_shard,
                    }
                })
                await ws.send(msg)
                print(f"[jetstream] Sent {len(dids_shard)} wantedDids via options_update", flush=True)

            cursor_save_counter = 0
            try:
                async for message in ws:
                    if not self.running:
                        break

                    # Decompress if needed (use stream_reader to handle
                    # frames without content size in header)
                    if isinstance(message, bytes) and self.decompressor:
                        try:
                            with self.decompressor.stream_reader(message) as reader:
                                message = reader.read()
                        except Exception:
                            continue
                    if isinstance(message, bytes):
                        message = message.decode("utf-8")

                    try:
                        event = json.loads(message)
                    except json.JSONDecodeError:
                        continue

                    self._handle_event(event)

                    # Save cursor periodically
                    cursor_save_counter += 1
                    if cursor_save_counter >= 1000:
                        time_us = event.get("time_us")
                        if time_us:
                            self.last_cursor = time_us
                            save_cursor(time_us)
                        cursor_save_counter = 0
            finally:
                if ws in self._ws_connections:
                    self._ws_connections.remove(ws)

    def _handle_event(self, event):
        """Process a single Jetstream event."""
        if event.get("kind") != "commit":
            return

        commit = event.get("commit", {})
        operation = commit.get("operation", "")
        collection = commit.get("collection", "")

        # Handle deletions — remove from buffer and queue DB delete
        if operation == "delete":
            did = event.get("did", "")
            rkey = commit.get("rkey", "")
            if collection == "app.bsky.feed.post":
                uri = f"at://{did}/app.bsky.feed.post/{rkey}"
                with self.buffer_lock:
                    self.buffer.pop(uri, None)
                with self._delete_lock:
                    self._pending_deletes.append(("post", uri))
            elif collection == "app.bsky.feed.repost":
                repost_uri = f"at://{did}/app.bsky.feed.repost/{rkey}"
                # Scan buffer to remove the repost (buffer key uses reposter::subject)
                with self.buffer_lock:
                    to_remove = None
                    for key, (rec, _) in self.buffer.items():
                        if rec.get("repost_uri") == repost_uri:
                            to_remove = key
                            break
                    if to_remove:
                        self.buffer.pop(to_remove, None)
                with self._delete_lock:
                    self._pending_deletes.append(("repost_by_uri", repost_uri))
            return

        if operation != "create":
            return

        self.stats["events"] += 1

        with self.buffer_lock:
            if collection == "app.bsky.feed.post":
                rec = parse_post_event(event)
                buf_key = rec["uri"]
                if buf_key not in self.buffer:
                    self.buffer[buf_key] = (rec, time.monotonic())
                    self.stats["buffered"] += 1

            elif collection == "app.bsky.feed.repost":
                rec = parse_repost_event(event)
                buf_key = f"{rec['reposter_did']}::{rec['subject_uri']}"
                if buf_key not in self.buffer:
                    self.buffer[buf_key] = (rec, time.monotonic())
                    self.stats["buffered"] += 1

    async def _process_buffer_loop(self):
        """Periodically process matured buffer entries."""
        conn = sqlite3.connect(DB_PATH)
        init_db(conn)
        prune_counter = 0

        try:
            while self.running:
                await asyncio.sleep(5)  # check every 5 seconds
                now_mono = time.monotonic()

                # Re-authenticate every 45 minutes
                loop = asyncio.get_event_loop()
                if now_mono - self.last_auth > 2700:
                    try:
                        await loop.run_in_executor(None, self._authenticate)
                    except Exception as e:
                        print(f"[auth] Re-auth failed: {e}", flush=True)

                # Process pending deletes (even when no matured posts)
                with self._delete_lock:
                    deletes = list(self._pending_deletes)
                    self._pending_deletes.clear()
                if deletes:
                    for del_type, del_val in deletes:
                        if del_type == "post":
                            conn.execute("DELETE FROM posts WHERE uri = ?", (del_val,))
                        elif del_type == "repost_by_uri":
                            conn.execute("DELETE FROM reposts WHERE repost_uri = ?", (del_val,))
                    conn.commit()

                # Collect matured entries (don't remove from buffer yet)
                matured = []
                matured_keys = []
                with self.buffer_lock:
                    for key, (rec, discovered) in self.buffer.items():
                        if now_mono - discovered >= DELAY_SECONDS:
                            matured_keys.append(key)
                            matured.append(rec)
                        else:
                            break  # OrderedDict: rest are newer

                if not matured:
                    # Prune periodically even when idle
                    prune_counter += 1
                    if prune_counter >= 360:  # every ~30 min
                        prune_old(conn, PRUNE_HOURS)
                        prune_stale_follows(conn)
                        prune_counter = 0
                    continue

                # Collect URIs to re-fetch engagement for
                uris_to_fetch = set()
                for rec in matured:
                    if rec["type"] == "post":
                        uris_to_fetch.add(rec["uri"])
                    elif rec["type"] == "repost":
                        uris_to_fetch.add(rec["subject_uri"])

                # Batch T5 re-fetch
                t5_data = {}
                if uris_to_fetch:
                    try:
                        t5_data = await loop.run_in_executor(
                            None, refetch_engagement, self.token, list(uris_to_fetch)
                        )
                    except Exception as e:
                        print(f"[refetch] T5 failed: {e}", flush=True)

                # Score and store
                stored = 0
                dropped = 0
                for rec in matured:
                    self.stats["scored"] += 1

                    if rec["type"] == "post":
                        t5 = t5_data.get(rec["uri"], {
                            "likeCount": 0, "repostCount": 0,
                            "replyCount": 0, "quoteCount": 0,
                        })
                        post_score = score_post(rec, t5)
                        if post_score >= SCORE_FLOOR:
                            store_post_db(conn, rec, post_score)
                            stored += 1
                        else:
                            dropped += 1

                    elif rec["type"] == "repost":
                        # Get original post data for decay and media flags
                        subject_data = t5_data.get(rec["subject_uri"], {})
                        if subject_data.get("createdAt"):
                            rec["original_created_at"] = subject_data["createdAt"]

                        # Get media flags from the original post if available
                        orig_post = conn.execute(
                            "SELECT has_images, has_link FROM posts WHERE uri = ?",
                            (rec["subject_uri"],)
                        ).fetchone()
                        if orig_post:
                            rec["has_images"] = bool(orig_post[0])
                            rec["has_link"] = bool(orig_post[1])
                        else:
                            # Fall back to media flags from getPosts response
                            rec["has_images"] = subject_data.get("has_images", False)
                            rec["has_link"] = subject_data.get("has_link", False)

                        t5 = {
                            "likeCount":   subject_data.get("likeCount", 0),
                            "repostCount": subject_data.get("repostCount", 0),
                            "replyCount":  subject_data.get("replyCount", 0),
                        }
                        repost_score = score_post(rec, t5)
                        if repost_score >= SCORE_FLOOR:
                            store_repost_db(conn, rec, repost_score)
                            stored += 1
                        else:
                            dropped += 1

                # Batch commit all stored posts/reposts
                conn.commit()

                # Now safe to drain buffer (data is persisted)
                with self.buffer_lock:
                    for key in matured_keys:
                        self.buffer.pop(key, None)

                # Log
                with self.buffer_lock:
                    buf_size = len(self.buffer)
                print(
                    f"[ingest] Processed {len(matured)} matured: "
                    f"+{stored} stored, -{dropped} dropped, "
                    f"{buf_size} buffered, {self.stats['events']} total events",
                    flush=True
                )

                # Prune periodically
                prune_counter += 1
                if prune_counter >= 360:
                    prune_old(conn, PRUNE_HOURS)
                    prune_stale_follows(conn)
                    # Rebuild interesting_dids from current follows cache
                    # Query inside the lock to prevent TOCTOU race with follows fetch
                    with self.interesting_dids_lock:
                        fresh_dids = set(
                            r[0] for r in conn.execute(
                                "SELECT DISTINCT follows_did FROM follows_cache"
                            ).fetchall()
                        )
                        removed = len(self.interesting_dids) - len(fresh_dids)
                        self.interesting_dids.clear()
                        self.interesting_dids.update(fresh_dids)
                    if removed > 0:
                        print(f"[housekeeping] Pruned {removed} stale DIDs from filter", flush=True)
                        self.request_dids_update()
                    prune_counter = 0
        finally:
            conn.close()

    async def _listen_for_updates(self):
        """Listen for wantedDids update requests from other threads."""
        while self.running:
            await self._update_event.wait()
            self._update_event.clear()

            with self.interesting_dids_lock:
                all_dids = list(self.interesting_dids)

            # Send options_update to all active connections
            # If we now exceed 10K, we need to reconnect with new sharding
            if len(all_dids) > JETSTREAM_MAX_DIDS * len(self._ws_connections):
                print(f"[jetstream] Need more shards for {len(all_dids)} DIDs, "
                      f"triggering reconnect ...", flush=True)
                # Force reconnect by closing connections
                for ws in list(self._ws_connections):
                    await ws.close()
                return

            # Distribute DIDs across existing connections
            shard_size = max(1, math.ceil(len(all_dids) / max(1, len(self._ws_connections))))
            for i, ws in enumerate(list(self._ws_connections)):
                shard = all_dids[i * shard_size:(i + 1) * shard_size]
                if not shard:
                    continue
                try:
                    msg = json.dumps({
                        "type": "options_update",
                        "payload": {
                            "wantedCollections": [
                                "app.bsky.feed.post",
                                "app.bsky.feed.repost",
                            ],
                            "wantedDids": shard,
                        }
                    })
                    await ws.send(msg)
                except Exception as e:
                    print(f"[jetstream] options_update failed: {e}", flush=True)


# ─────────────────────────────────────────────
#  HTTP SERVER (Feed Generator Protocol)
# ─────────────────────────────────────────────

_local = threading.local()


def get_db():
    if not hasattr(_local, "conn"):
        _local.conn = sqlite3.connect(DB_PATH)
        _configure_conn(_local.conn)
    return _local.conn


class FeedHandler(BaseHTTPRequestHandler):
    publisher_did = None
    consumer = None  # set from main
    interesting_dids = None
    interesting_dids_lock = None

    def log_message(self, format, *args):
        pass  # quiet

    def _send_json(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?")[0]

        if path == "/.well-known/did.json":
            self._handle_did_doc()
        elif path == "/xrpc/app.bsky.feed.describeFeedGenerator":
            self._handle_describe()
        elif path == "/xrpc/app.bsky.feed.getFeedSkeleton":
            self._handle_get_feed_skeleton()
        elif path == "/health":
            self._send_json({"status": "ok"})
        elif path == "/stats":
            self._handle_stats()
        else:
            self._send_json({"error": "Not Found"}, 404)

    def _handle_did_doc(self):
        self._send_json({
            "@context": ["https://www.w3.org/ns/did/v1"],
            "id": f"did:web:{FEED_HOSTNAME}",
            "service": [{
                "id": "#bsky_fg",
                "type": "BskyFeedGenerator",
                "serviceEndpoint": f"https://{FEED_HOSTNAME}",
            }],
        })

    def _handle_describe(self):
        self._send_json({
            "did": f"did:web:{FEED_HOSTNAME}",
            "feeds": [{
                "uri": f"at://{self.publisher_did}/app.bsky.feed.generator/{FEED_NAME}"
            }],
        })

    def _handle_get_feed_skeleton(self):
        # Parse query params
        query = urllib.parse.urlparse(self.path).query
        params = urllib.parse.parse_qs(query)
        limit = min(int(params.get("limit", ["50"])[0]), 100)
        cursor = params.get("cursor", [None])[0]

        # Verify JWT to get requester's DID
        auth_header = self.headers.get("Authorization", "")
        requester_did = verify_request_jwt(auth_header)

        if not requester_did:
            # No auth or invalid — return empty feed
            # (could also return 401, but empty is more graceful)
            self._send_json({"feed": []})
            return

        # Get requester's follows
        conn = get_db()
        follows = get_or_fetch_follows(
            conn, requester_did,
            self.interesting_dids_lock, self.interesting_dids,
            self.consumer.request_dids_update if self.consumer else None,
        )

        if not follows:
            self._send_json({"feed": []})
            return

        # Query personalized feed
        rows = get_user_feed(conn, follows, cursor, limit)

        feed = []
        for post_uri, indexed_at, repost_uri in rows:
            entry = {"post": post_uri}
            if repost_uri:
                entry["reason"] = {
                    "$type": "app.bsky.feed.defs#skeletonReasonRepost",
                    "repost": repost_uri,
                }
            feed.append(entry)

        result = {"feed": feed}
        if rows:
            result["cursor"] = rows[-1][1]  # indexed_at of last row

        self._send_json(result)

    def _handle_stats(self):
        conn = get_db()
        post_count = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
        repost_count = conn.execute("SELECT COUNT(*) FROM reposts").fetchone()[0]
        user_count = conn.execute(
            "SELECT COUNT(DISTINCT user_did) FROM follows_cache"
        ).fetchone()[0]

        with self.interesting_dids_lock:
            dids_count = len(self.interesting_dids)

        self._send_json({
            "posts": post_count,
            "reposts": repost_count,
            "active_users": user_count,
            "interesting_dids": dids_count,
            "jetstream_connected": self.consumer.connected if self.consumer else False,
            "ingest": self.consumer.stats if self.consumer else {},
        })


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────

def main():
    if not BSKY_APP_PASSWORD:
        print("ERROR: Set BSKY_APP_PASSWORD environment variable", file=sys.stderr)
        sys.exit(1)

    # Check dependencies
    missing = []
    if websockets is None:
        missing.append("websockets")
    if zstd is None:
        missing.append("zstandard")
    if not HAS_ATPROTO:
        missing.append("atproto")
    if missing:
        print(f"ERROR: Missing dependencies: {', '.join(missing)}", file=sys.stderr)
        print(f"  pip install {' '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    print("=" * 50)
    print("  Bluesky Multi-User Feed Generator")
    print("=" * 50)
    print(f"  Handle:    {BSKY_HANDLE}")
    print(f"  Hostname:  {FEED_HOSTNAME}")
    print(f"  Port:      {FEED_PORT}")
    print(f"  Feed:      {FEED_NAME}")
    print(f"  Delay:     {DELAY_SECONDS}s")
    print(f"  Floor:     {SCORE_FLOOR}")
    print(f"  Prune:     {PRUNE_HOURS}h")
    print(f"  Half-life: {REPOST_HALF_LIFE}h")
    print()

    # Initialize database
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
    post_count = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
    repost_count = conn.execute("SELECT COUNT(*) FROM reposts").fetchone()[0]
    print(f"[db] {post_count} posts, {repost_count} reposts in database", flush=True)

    # Load existing interesting DIDs from follows cache
    interesting_dids = set()
    rows = conn.execute("SELECT DISTINCT follows_did FROM follows_cache").fetchall()
    for r in rows:
        interesting_dids.add(r[0])
    print(f"[db] {len(interesting_dids)} interesting DIDs from follows cache", flush=True)
    conn.close()

    interesting_dids_lock = threading.Lock()

    # Initialize JWT verification
    _init_jwt()

    # Start Jetstream consumer
    consumer = JetstreamConsumer(interesting_dids, interesting_dids_lock)
    consumer.start()

    # Wait for auth to complete (timeout after 30s)
    deadline = time.monotonic() + 30
    while consumer.publisher_did is None:
        if time.monotonic() > deadline:
            print("ERROR: Failed to authenticate within 30s", file=sys.stderr)
            sys.exit(1)
        time.sleep(0.2)

    # Configure HTTP handler
    FeedHandler.publisher_did = consumer.publisher_did
    FeedHandler.consumer = consumer
    FeedHandler.interesting_dids = interesting_dids
    FeedHandler.interesting_dids_lock = interesting_dids_lock

    print(f"[server] Publisher DID: {consumer.publisher_did}", flush=True)
    print(f"[server] Feed URI: at://{consumer.publisher_did}/app.bsky.feed.generator/{FEED_NAME}", flush=True)

    # Start HTTP server
    class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
        daemon_threads = True

    server = ThreadingHTTPServer(("0.0.0.0", FEED_PORT), FeedHandler)
    print(f"[server] Listening on 0.0.0.0:{FEED_PORT}", flush=True)
    print(f"[server] DID doc: https://{FEED_HOSTNAME}/.well-known/did.json", flush=True)
    print()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[server] Shutting down ...", flush=True)
        consumer.running = False
        server.shutdown()


if __name__ == "__main__":
    main()
