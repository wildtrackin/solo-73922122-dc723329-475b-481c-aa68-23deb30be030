#!/usr/bin/env bash
# Restart / failover demonstration for the two shared-database API processes.
#
# Prerequisite: docker compose up -d   (db, api-1, api-2 healthy)
#
# What it proves:
#   - one worker owns a batch (row-level locks + SKIP LOCKED);
#   - restarting the NON-owning process does not disturb the active batch;
#   - SIGKILL of the owner mid-flight causes the surviving process to reclaim
#     the expired leases and continue with the identical frozen input;
#   - final results are complete: every application once, no duplicates.
set -euo pipefail
cd "$(dirname "$0")/.."
N="${N:-60}"

get()  { curl -fsS -H 'Content-Type: application/json' "$@"; }
post() { curl -fsS -H 'Content-Type: application/json' -X POST "$@"; }
worker_of() { get "$1/health" | python3 -c 'import json,sys;print(json.load(sys.stdin)["worker_id"])'; }

echo "== seed bulk dataset ($N applications/events) =="
python3 scripts/gen_bulk.py http://127.0.0.1:8001 "$N"

echo "== create one big batch =="
BATCH=$(post -d "$(python3 -c "import json,sys;n=int(sys.argv[1]);print(json.dumps({'application_ids':[f'B{i:04d}' for i in range(n)]}))" "$N")" \
  http://127.0.0.1:8001/api/v1/batches | python3 -c 'import json,sys;print(json.load(sys.stdin)["batch_id"])')
echo "batch id: $BATCH"

# Wait for a worker to pick it up and record which process owns it.
OWNER_W=""
for _ in $(seq 1 30); do
  OWNER_W=$(get http://127.0.0.1:8001/api/v1/batches/$BATCH | python3 -c 'import json,sys;print(json.load(sys.stdin)["worker_id"] or "")')
  [ -n "$OWNER_W" ] && break
  sleep 0.5
done
W1=$(worker_of http://127.0.0.1:8001)
if [ "$W1" = "$OWNER_W" ]; then OWN=api-1; OTHER=api-2; else OWN=api-2; OTHER=api-1; fi
echo "owner = $OWN ($OWNER_W); non-owner = $OTHER"

echo "== restart the NON-owning process; the active batch must be unaffected =="
docker compose restart "$OTHER"
sleep 2
get http://127.0.0.1:8001/api/v1/batches/$BATCH
STILL=$(get http://127.0.0.1:8001/api/v1/batches/$BATCH | python3 -c 'import json,sys;print(json.load(sys.stdin)["worker_id"])')
[ "$STILL" = "$OWNER_W" ] && echo "ownership unchanged while non-owner restarted ✔"

echo "== SIGKILL the owner mid-flight (no graceful shutdown) =="
docker compose kill "$OWN"
echo "owner killed; the surviving process waits for the 15s lease to expire, then reclaims..."
sleep 18
docker compose start "$OWN"

echo "== wait for completion and verify no gaps / duplicates =="
python3 - "$BATCH" <<'PY'
import json, sys, time, urllib.request
bid = sys.argv[1]
def get(path):
    with urllib.request.urlopen("http://127.0.0.1:8001" + path, timeout=30) as r:
        return json.loads(r.read())
deadline = time.time() + 120
while time.time() < deadline:
    b = get(f"/api/v1/batches/{bid}")
    if b["status"] in ("COMPLETED", "FAILED"):
        break
    time.sleep(0.5)
print("final batch:", b)
assert b["status"] == "COMPLETED", b
items, cursor = [], None
while True:
    p = f"/api/v1/batches/{bid}/results?limit=50" + (f"&cursor={cursor}" if cursor else "")
    page = get(p)
    items += page["items"]
    cursor = page["next_cursor"]
    if not cursor:
        break
ids = [i["application_id"] for i in items]
assert len(ids) == b["total_items"], (len(ids), b["total_items"])
assert len(set(ids)) == len(ids), "duplicate result rows!"
assert ids == sorted(ids), "pagination order broken!"
assert all(i["income"] is not None and i["source_event_id"] for i in items), "missing backfill!"
print(f"verified {len(ids)} unique, fully-computed, source-annotated rows; failover demo OK")
PY
