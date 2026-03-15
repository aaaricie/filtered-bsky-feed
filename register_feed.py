"""
Register (or update) the custom feed with Bluesky.

Run once after setting up the server:
    BSKY_APP_PASSWORD=xxx python register_feed.py

This creates the app.bsky.feed.generator record in your repo so that
Bluesky clients can discover and subscribe to your feed.
"""

import urllib.request
import urllib.parse
import json
import os
import sys
import datetime

BSKY_API     = "https://bsky.social/xrpc"
HANDLE       = os.environ.get("BSKY_HANDLE", "your-handle.bsky.social")
APP_PASSWORD = os.environ.get("BSKY_APP_PASSWORD", "")
FEED_HOSTNAME = os.environ.get("FEED_HOSTNAME", "feed.example.com")
FEED_NAME     = os.environ.get("FEED_NAME", "filtered-following")
FEED_DISPLAY_NAME = os.environ.get("FEED_DISPLAY_NAME", "Filtered Following")
FEED_DESCRIPTION  = os.environ.get("FEED_DESCRIPTION",
    "Your following timeline, filtered to surface interesting and engaging posts. "
    "Personalized to your follows — works for everyone.")


def _request(url, *, token=None, payload=None):
    data = json.dumps(payload).encode() if payload else None
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=headers,
                                method="POST" if data else "GET")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())


def main():
    if not APP_PASSWORD:
        print("ERROR: Set BSKY_APP_PASSWORD environment variable", file=sys.stderr)
        sys.exit(1)

    # Authenticate
    print(f"Authenticating as {HANDLE} ...")
    resp = _request(
        f"{BSKY_API}/com.atproto.server.createSession",
        payload={"identifier": HANDLE, "password": APP_PASSWORD}
    )
    token = resp["accessJwt"]
    did = resp["did"]

    feed_uri = f"at://{did}/app.bsky.feed.generator/{FEED_NAME}"
    print(f"Feed URI: {feed_uri}")
    print(f"Feed DID: did:web:{FEED_HOSTNAME}")

    # Create the feed generator record
    record = {
        "$type": "app.bsky.feed.generator",
        "did": f"did:web:{FEED_HOSTNAME}",
        "displayName": FEED_DISPLAY_NAME,
        "description": FEED_DESCRIPTION,
        "createdAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }

    print(f"\nCreating feed record '{FEED_NAME}' ...")
    try:
        resp = _request(
            f"{BSKY_API}/com.atproto.repo.putRecord",
            token=token,
            payload={
                "repo": did,
                "collection": "app.bsky.feed.generator",
                "rkey": FEED_NAME,
                "record": record,
            }
        )
        print(f"Done! Feed registered at: {resp.get('uri', feed_uri)}")
        print(f"\nYou can now find it at: https://bsky.app/profile/{HANDLE}/feed/{FEED_NAME}")
    except Exception as e:
        print(f"Failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
