#!/usr/bin/env python3
"""Generate a large fictional dataset via the API, for the restart demo."""
from __future__ import annotations

import sys
import urllib.request
import json

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8001"
N = int(sys.argv[2]) if len(sys.argv) > 2 else 60


def post(path: str, body: dict) -> dict:
    req = urllib.request.Request(
        BASE + path, data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"}, method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def main() -> None:
    apps = [
        {"application_id": f"B{i:04d}", "customer_id": f"CB{i:04d}",
         "application_time": "2026-05-01T00:00:00Z"}
        for i in range(N)
    ]
    events = [
        {"event_id": f"EB{i:04d}", "customer_id": f"CB{i:04d}",
         "business_time": "2026-04-01T00:00:00Z",
         "known_time": "2026-04-02T00:00:00Z",
         "income": 20000 + i, "withdrawn": False}
        for i in range(N)
    ]
    print(post("/api/v1/imports/applications", {"applications": apps}))
    print(post("/api/v1/imports/credit-events", {"events": events}))
    print("generated", N, "bulk applications/events")


if __name__ == "__main__":
    main()
