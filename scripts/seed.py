#!/usr/bin/env python3
"""Load the fictional seed data (applications + credit events) into the API.

Usage: python scripts/seed.py [BASE_URL]
"""
from __future__ import annotations

import json
import pathlib
import sys
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8001"
DATA = pathlib.Path(__file__).resolve().parent.parent / "data"


def post(path: str, body: dict) -> dict:
    req = urllib.request.Request(
        BASE + path,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return {"status": resp.status, "body": json.loads(resp.read())}
    except urllib.error.HTTPError as exc:
        return {"status": exc.code, "body": json.loads(exc.read())}


def main() -> None:
    apps = json.loads((DATA / "seed_applications.json").read_text())
    events = json.loads((DATA / "seed_events.json").read_text())

    r1 = post("/api/v1/imports/applications", apps)
    print("applications:", r1["status"], r1["body"])
    r2 = post("/api/v1/imports/credit-events", events)
    print("credit events:", r2["status"], r2["body"])


if __name__ == "__main__":
    main()
