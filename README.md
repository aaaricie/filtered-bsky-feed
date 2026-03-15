# Filtered Bluesky Feed

A custom Bluesky feed generator that filters your Following timeline to surface the most engaging posts. Works for any Bluesky user automatically — no login or OAuth required.

## How it works

1. **Jetstream firehose** — Connects to Bluesky's [Jetstream](https://github.com/bluesky-social/jetstream) WebSocket to receive posts and reposts in real time from accounts that any active user follows.

2. **5-minute buffer** — Every post is held for 5 minutes before being scored, giving it time to accumulate early engagement signals.

3. **T5 engagement scoring** — After the buffer period, engagement is re-fetched via the `getPosts` API and each post is scored based on likes, reposts, replies, and media presence.

4. **Per-user personalization** — When you open the feed, the server identifies you via the JWT your Bluesky client sends automatically, fetches your follow list via the public API, and filters the scored posts to only those from accounts *you* follow.

### Scoring formula

```
Raw engagement (subject to decay for reposts):
  min(likes, 150) + reposts × 3 + replies × 2 + likes × 2 (velocity)

Bonuses (not decayed):
  +15 if has link and likes ≥ 1
  +10 if has images and likes ≥ 1

Penalties (not decayed):
  -15 for reposts
  -25 for replies (not self-replies)
   -5 for self-replies (threads)

Floor: 17 (posts scoring below this are dropped)
```

**Repost age decay:** For reposts, the raw engagement is multiplied by `0.5 ^ (original_post_age_hours / 6)`. A 6-hour-old post keeps 50% of its raw score, a 12-hour-old post keeps 25%, etc. Bonuses and penalties are not affected by decay.

## Setup

### Requirements

- Python 3.10+
- A Bluesky account with an [app password](https://bsky.app/settings/app-passwords)
- A domain pointed at your server (e.g. `feed.yourdomain.com`)

### Quick start

```bash
# Clone
git clone https://github.com/yourusername/filtered-bsky-feed.git
cd filtered-bsky-feed

# Install dependencies
pip install -r requirements.txt

# Configure
cp .env.example .env
# Edit .env with your credentials and domain
nano .env

# Register the feed with Bluesky (one time only)
source .env && export BSKY_APP_PASSWORD BSKY_HANDLE FEED_HOSTNAME FEED_NAME
python register_feed.py

# Run
python feed_server.py
```

### Ubuntu server deployment

Run the included deploy script for a systemd-based setup:

```bash
chmod +x deploy.sh
./deploy.sh
```

This installs the service, creates a `.env` template, and sets up auto-restart. Follow the on-screen instructions to configure and start it.

### DNS / Reverse proxy

Your `FEED_HOSTNAME` domain must serve:
- `/.well-known/did.json` — the DID document (served by the feed server)
- `/xrpc/app.bsky.feed.*` — the feed generator endpoints

If using Cloudflare, set SSL mode to **Flexible** and add an Origin Rule to route traffic to your server's port (default 8000).

## Endpoints

| Path | Description |
|------|-------------|
| `/health` | Health check |
| `/stats` | Server statistics (posts stored, active users, Jetstream status) |
| `/.well-known/did.json` | DID document for `did:web:` resolution |
| `/xrpc/app.bsky.feed.describeFeedGenerator` | Feed metadata |
| `/xrpc/app.bsky.feed.getFeedSkeleton` | The feed itself (requires JWT from Bluesky client) |

## Configuration

All settings are configured via environment variables (or `.env` file):

| Variable | Default | Description |
|----------|---------|-------------|
| `BSKY_HANDLE` | `your-handle.bsky.social` | Your Bluesky handle |
| `BSKY_APP_PASSWORD` | — | App password (required) |
| `FEED_HOSTNAME` | `feed.example.com` | Domain for the feed server |
| `FEED_PORT` | `8000` | HTTP port |
| `FEED_NAME` | `filtered-following` | Feed record key |
| `DELAY_SECONDS` | `300` | Buffer time before scoring (seconds) |
| `PRUNE_HOURS` | `48` | How long scored posts are kept |
| `FOLLOWS_CACHE_TTL` | `3600` | How often user follow lists are refreshed (seconds) |
| `REPOST_HALF_LIFE` | `6.0` | Repost age decay half-life (hours) |
| `JETSTREAM_URL` | `wss://jetstream2.us-east.bsky.network/subscribe` | Jetstream endpoint |

## Architecture

Single Python process with three concurrent components:

- **Jetstream consumer** (asyncio) — WebSocket connection(s) to the firehose, buffers events, processes T5 scoring. Automatically shards across multiple connections if tracking >10,000 accounts.
- **HTTP server** (threaded) — Serves the feed generator protocol. Each request gets its own thread and database connection.
- **Housekeeping** — Prunes old posts, refreshes follow caches, rebuilds the Jetstream DID filter.

Data is stored in SQLite (WAL mode) and persists across restarts. The Jetstream cursor is saved to disk for crash recovery.
