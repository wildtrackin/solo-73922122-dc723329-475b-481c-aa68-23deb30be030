#!/usr/bin/env python3
"""End-to-end demonstration against the running two-process API.

Covers every requirement:
  1. import applications + credit-event versions (with revisions / withdrawal /
     same-business-time tie);
  2. whole-batch rejection of same event_id+known_time with different values;
  3. batch generation freezes the inputs accepted at that instant;
  4. point-in-time backfill results with the exact source version annotated;
  5. a late correction imported afterwards cannot change the completed batch;
  6. a second batch created after the correction sees it;
  7. keyset pagination;
  8. both API processes cooperate (health/worker ids shown).

Usage: python scripts/demo.py [BASE_URL_1] [BASE_URL_2]
"""
from __future__ import annotations

import json
import pathlib
import sys
import time
import urllib.error
import urllib.request

BASE1 = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8001"
BASE2 = sys.argv[2] if len(sys.argv) > 2 else "http://127.0.0.1:8002"
DATA = pathlib.Path(__file__).resolve().parent.parent / "data"


def call(base: str, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        base + path, data=data,
        headers={"Content-Type": "application/json"}, method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def heading(title: str) -> None:
    print("\n" + "=" * 72 + f"\n{title}\n" + "=" * 72)


def wait_completed(base: str, batch_id: str, timeout: float = 60) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, batch = call(base, "GET", f"/api/v1/batches/{batch_id}")
        if batch["status"] in ("COMPLETED", "FAILED"):
            return batch
        time.sleep(0.2)
    raise TimeoutError(f"batch {batch_id} did not finish")


def fetch_all_results(base: str, batch_id: str, limit: int = 2) -> list[dict]:
    items, cursor = [], None
    while True:
        path = f"/api/v1/batches/{batch_id}/results?limit={limit}"
        if cursor:
            path += f"&cursor={cursor}"
        status, page = call(base, "GET", path)
        if status != 200:
            raise AssertionError(f"results endpoint returned {status}: {page}")
        items.extend(page["items"])
        cursor = page["next_cursor"]
        if not cursor:
            return items


def main() -> None:
    heading("0. 两个共库 API 进程健康检查 / worker 身份")
    for base in (BASE1, BASE2):
        print(base, "->", call(base, "GET", "/health"))

    heading("1. 导入虚构申请与征信事件版本")
    apps = json.loads((DATA / "seed_applications.json").read_text())
    events = json.loads((DATA / "seed_events.json").read_text())
    print("applications:", call(BASE1, "POST", "/api/v1/imports/applications", apps))
    print("events      :", call(BASE1, "POST", "/api/v1/imports/credit-events", events))

    heading("2. 同事件同获知时间异值 -> 整批拒绝（无任何行落库）")
    conflict = json.loads((DATA / "conflict_events.json").read_text())
    print("conflict import:", call(BASE2, "POST", "/api/v1/imports/credit-events", conflict))
    _, stored = call(BASE1, "GET", "/api/v1/events")
    print("E9 rows stored (expect 0):", [r for r in stored if r["event_id"] == "E9"])

    heading("3. 生成批次 B1：固化受理时刻已提交的输入")
    code, b1 = call(BASE1, "POST", "/api/v1/batches",
                    {"application_ids": ["A1", "A2", "A3", "A4", "A5"]})
    print(code, b1)
    b1_id = b1["batch_id"]
    final = wait_completed(BASE2, b1_id)
    print("final status (polled via the OTHER process):", final)

    heading("4. B1 时点回填结果（含来源版本）")
    results = fetch_all_results(BASE1, b1_id, limit=2)
    for r in results:
        print(f"{r['application_id']}: income={r['income']!s:>6}  "
              f"basis={r['basis']:>12}  source={r['source_event_id']}  "
              f"known_time={r['source_version_known_time']}")

    expectations = {
        # A1: E1 的 01-12 修订晚于申请 01-11 -> 只能看到 50000 版本
        "A1": (50000, "E1"),
        # A2: E3 已在 02-10 撤回，且无其他有效事件 -> 空
        "A2": (None, None),
        # A3: 唯一事件 E4 的业务时间晚于申请 -> 空
        "A3": (None, None),
        # A4: E1(01-01) 与 E2(02-15) 都有效，取业务时间最近的 E2
        "A4": (60000, "E2"),
        # A5: E5A/E5B 业务时间相同并列，按事件 ID 取 E5A
        "A5": (30000, "E5A"),
    }
    by_app = {r["application_id"]: r for r in results}
    for app_id, (income, event_id) in expectations.items():
        row = by_app[app_id]
        assert row["income"] == income and row["source_event_id"] == event_id, (
            f"{app_id} expected {(income, event_id)} got "
            f"{(row['income'], row['source_event_id'])}"
        )
    print("all point-in-time assertions passed ✔")

    heading("5. 批次完成后再导入迟到更正 -> 不得改变 B1")
    late = json.loads((DATA / "late_correction.json").read_text())
    print("late correction:", call(BASE2, "POST", "/api/v1/imports/credit-events", late))
    again = fetch_all_results(BASE1, b1_id, limit=2)
    assert again == results, "completed batch changed!"
    print("B1 results unchanged ✔  (A1 still", by_app["A1"]["income"], ")")

    heading("6. 更正之后生成 B2 -> 新批次可以看到更正")
    _, b2 = call(BASE1, "POST", "/api/v1/batches", {"application_ids": ["A1"]})
    wait_completed(BASE1, b2["batch_id"])
    b2_rows = fetch_all_results(BASE2, b2["batch_id"])
    print("B2 A1:", b2_rows[0]["income"], "from", b2_rows[0]["source_event_id"],
          "version", b2_rows[0]["source_version_known_time"])
    assert b2_rows[0]["income"] == 51000
    print("new batch sees the corrected version (51000) ✔")

    heading("7. 未完成批次不开放结果查询")
    _, b3 = call(BASE1, "POST", "/api/v1/batches", {"application_ids": ["A2"]})
    # 立即查询（极可能仍在处理中；若已完成则跳过该断言）
    code, body = call(BASE1, "GET", f"/api/v1/batches/{b3['batch_id']}/results")
    _, b3info = call(BASE1, "GET", f"/api/v1/batches/{b3['batch_id']}")
    if b3info["status"] != "COMPLETED":
        assert code == 409, (code, body)
        print("pending batch correctly refused with 409 ✔", body)
    else:
        print("batch finished too fast to demonstrate; 409 guard is unit-covered")
    wait_completed(BASE1, b3["batch_id"])

    heading("8. 批次列表（成功批次不再变化）")
    _, batches = call(BASE1, "GET", "/api/v1/batches")
    for b in batches:
        print(b["batch_id"], b["status"], f"{b['processed_items']}/{b['total_items']}",
              "worker:", b["worker_id"])

    print("\nDEMO FINISHED OK")


if __name__ == "__main__":
    main()
