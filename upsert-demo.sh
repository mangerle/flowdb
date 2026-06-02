#!/usr/bin/env bash
set -euo pipefail

ADDR="${1:-http://localhost:8080}"

ok() { printf "\033[32m[OK]\033[0m %s\n" "$1"; }
section() { printf "\n\033[1;33m=== %s ===\033[0m\n" "$1"; }

section "0. Clear old data"
curl -sf -X POST "$ADDR/admin/gc" | jq .
ok "gc triggered"

qget() { curl -sf "$ADDR/query?$1" | jq '{count,records: [.records[] | {key,ts,value: (.value | @base64d)}]}'; }

section "1. Health check"
curl -sf "$ADDR/health" | jq .
ok "server is alive"

section "2. Insert batch 1: call-* (3 records)"
TS=$(($(date +%s) * 1000000))
curl -sf -X POST "$ADDR/write" -H 'Content-Type: application/json' -d "$(cat <<EOF
{
  "records": [
    {"key":"call-alice","ts":$TS,"value":"hello alice"},
    {"key":"call-bob","ts":$((TS+1000000)),"value":"hello bob"},
    {"key":"call-charlie","ts":$((TS+2000000)),"ttl_secs":3600,"value":"hello charlie"}
  ]
}
EOF
)" | jq .
ok "3 call records inserted"

section "3. Insert batch 2: metric-* (10 records)"
RECORDS=""
for i in $(seq 0 4); do
  T=$((TS + i * 500000))
  CPU=$(awk "BEGIN{srand($i); printf \"%.1f\", 40+rand()*50}")
  MEM=$(awk "BEGIN{srand($i+100); printf \"%.1f\", 50+rand()*40}")
  RECORDS="${RECORDS}{\"key\":\"metric-cpu\",\"ts\":$T,\"value\":\"$CPU\"},"
  RECORDS="${RECORDS}{\"key\":\"metric-mem\",\"ts\":$T,\"value\":\"$MEM\"},"
done
RECORDS="[${RECORDS%,}]"
curl -sf -X POST "$ADDR/write" -H 'Content-Type: application/json' -d "{\"records\":$RECORDS}" | jq .
ok "10 metric records inserted"

section "4. Insert batch 3: event-* (3 records, with TTL)"
EVENTS=""
for i in $(seq 0 2); do
  T=$((TS + i * 1000000))
  EVENTS="${EVENTS}{\"key\":\"event-login\",\"ts\":$T,\"ttl_secs\":300,\"value\":\"user_$i\"},"
done
EVENTS="[${EVENTS%,}]"
curl -sf -X POST "$ADDR/write" -H 'Content-Type: application/json' -d "{\"records\":$EVENTS}" | jq .
ok "3 event records inserted (TTL=300s)"

section "5. Upsert: overwrite call-alice with new value"
curl -sf -X POST "$ADDR/write" -H 'Content-Type: application/json' -d "$(cat <<EOF
{"records":[{"key":"call-alice","ts":$TS,"value":"UPDATED: alice was here"}]}
EOF
)" | jq .
ok "call-alice upserted"

section "6. Patch: update call-bob value and TTL"
curl -sf -X PATCH "$ADDR/record" -H 'Content-Type: application/json' -d "$(cat <<EOF
{"key":"call-bob","ts":$((TS+1000000)),"value":"PATCHED: bob got a promotion","ttl_secs":7200}
EOF
)" | jq .
ok "call-bob patched"

section "7. Query: prefix=call- (verify upsert + patch)"
qget "prefix=call-"
ok "prefix query done"

section "8. Delete: remove call-charlie"
curl -sf -X DELETE "$ADDR/record?key=call-charlie&ts=$((TS+2000000))" | jq .
ok "call-charlie deleted"

section "9. Query: prefix=call- (verify delete)"
qget "prefix=call-"
ok "verify call-charlie is gone"

section "10. Query: prefix=metric-"
qget "prefix=metric-"
ok "metric query done"

section "11. Query: key range [call-a, call-d)"
qget "key_start=call-a&key_end=call-d"
ok "key range query done"

section "12. Query: time range"
qget "ts_start=$TS&ts_end=$((TS+2500000))"
ok "time range query done"

section "13. Query: prefix + time range"
qget "prefix=call-&ts_start=$TS&ts_end=$((TS+1500000))"
ok "combined query done"

section "14. Flush memtable to SSTable"
curl -sf -X POST "$ADDR/admin/flush" | jq .
ok "flush triggered"

section "15. Query after flush (data now from SST)"
qget "prefix=call-"
ok "post-flush query done"

section "16. Delete after flush (tombstone in SST)"
curl -sf -X DELETE "$ADDR/record?key=call-alice&ts=$TS" | jq .
ok "call-alice deleted (after flush)"

section "17. Query after SST delete"
qget "prefix=call-"
ok "verify call-alice is gone"

section "18. Stats"
curl -sf "$ADDR/stats" | jq '{
  total_written: .total_records_written,
  total_read: .total_records_read,
  memtable_records: .memtable_records,
  sst_count: .sstable_count,
  sst_bytes: .sstable_bytes
}'
ok "stats retrieved"

section "19. Trigger compaction"
curl -sf -X POST "$ADDR/admin/compact" | jq .
ok "compaction triggered"

section "20. Final query: all records (prefix='')"
qget "prefix="
ok "full scan done"

printf "\n\033[32mDemo complete! Visit %s/admin for the dashboard.\033[0m\n" "$ADDR"
