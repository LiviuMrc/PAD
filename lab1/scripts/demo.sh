#!/usr/bin/env bash
# Rulează automat scenariul de demo + eșec și salvează log-urile în docs/evidence/.
# Utilizare: ./scripts/demo.sh
set -euo pipefail
cd "$(dirname "$0")/.."

export BROKER_ADDR="${BROKER_ADDR:-127.0.0.1:7000}"
export HEALTH_ADDR="${HEALTH_ADDR:-127.0.0.1:7001}"
export MULTICAST_ADDR="${MULTICAST_ADDR:-239.0.0.1:9999}"
OUT=docs/evidence
mkdir -p bin "$OUT"
go build -o bin/ ./broker ./sender ./receiver

PIDS=()
cleanup() { kill "${PIDS[@]}" 2>/dev/null || true; rm -rf bin; }
trap cleanup EXIT

step() { echo; echo "### $*" | tee -a "$OUT/sender.log"; }

: > "$OUT/sender.log"
bin/broker > "$OUT/broker.log" 2>&1 & PIDS+=($!)
sleep 0.5

# starea inițială: 2 receivere online
bin/receiver -id receiver-1 -topics news -multicast > "$OUT/receiver-1.log" 2>&1 & PIDS+=($!)
bin/receiver -id receiver-2 -topics news,alerts -multicast > "$OUT/receiver-2-before.log" 2>&1 & R2=$!; PIDS+=($R2)
sleep 0.5

step "1. publicare cu ambele receivere online"
bin/sender -topic news -payload "mesaj 1 (ambii online)" >> "$OUT/sender.log" 2>&1

step "2. injectarea eșecului: receiver-2 este oprit"
kill "$R2"; sleep 0.3

step "3. publicare cât timp receiver-2 e offline"
bin/sender -topic news -payload "mesaj 2 (receiver-2 offline)" >> "$OUT/sender.log" 2>&1
bin/sender -topic alerts -payload "mesaj 3 (alerts, receiver-2 offline)" >> "$OUT/sender.log" 2>&1
curl -s "$HEALTH_ADDR/health/ready" | tee -a "$OUT/sender.log" || true

step "4. recuperarea: receiver-2 repornește cu același id"
bin/receiver -id receiver-2 -topics news,alerts -multicast > "$OUT/receiver-2-after.log" 2>&1 & PIDS+=($!)
sleep 0.5

step "5. multicast UDP pe topicul broadcast"
bin/sender -topic broadcast -payload "mesaj 4 (multicast)" >> "$OUT/sender.log" 2>&1

step "6. validare: XML invalid respins"
bin/sender -raw contracts/examples/invalid-type.xml >> "$OUT/sender.log" 2>&1 || true

step "7. același mesaj în XML și JSON"
bin/sender -topic news -payload "json demo" -json >> "$OUT/sender.log" 2>&1
sleep 0.5

echo
for f in receiver-1 receiver-2-before receiver-2-after; do
  echo "== $f"; grep '^>>>' "$OUT/$f.log" || echo "(nimic)"
done
echo
echo "Log-uri complete în $OUT/"
