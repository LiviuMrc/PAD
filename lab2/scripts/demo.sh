#!/usr/bin/env bash
# Rulează automat scenariul de demo + eșec și salvează log-urile în docs/evidence/.
# Utilizare: ./scripts/demo.sh   (rulează întâi ./scripts/gen.sh)
set -euo pipefail
cd "$(dirname "$0")/.."

export BROKER_ADDR="${BROKER_ADDR:-127.0.0.1:50061}"
OUT=docs/evidence
mkdir -p bin "$OUT"
go build -o bin/ ./broker ./sender ./receiver

PIDS=()
cleanup() { kill "${PIDS[@]}" 2>/dev/null || true; rm -rf bin; }
trap cleanup EXIT

step() { echo; echo "### $*" | tee -a "$OUT/sender.log"; }

: > "$OUT/sender.log"
bin/broker > "$OUT/broker.log" 2>&1 & BP=$!; PIDS+=($BP)
sleep 0.5
if ! kill -0 "$BP" 2>/dev/null; then
  echo "Brokerul nu a pornit:"; cat "$OUT/broker.log"; exit 1
fi

# starea inițială: 2 receivere online
bin/receiver -id receiver-1 -topics news > "$OUT/receiver-1.log" 2>&1 & R1=$!; PIDS+=($R1)
bin/receiver -id receiver-2 -topics news,alerts > "$OUT/receiver-2-before.log" 2>&1 & R2=$!; PIDS+=($R2)
sleep 0.5

step "1. Publish cu ambele receivere online"
bin/sender -topic news -payload "mesaj 1 (ambii online)" >> "$OUT/sender.log" 2>&1

step "2. injectarea eșecului: receiver-2 este oprit"
kill "$R2"; sleep 0.3

step "3. Publish cât timp receiver-2 e offline"
bin/sender -topic news -payload "mesaj 2 (receiver-2 offline)" >> "$OUT/sender.log" 2>&1
bin/sender -topic alerts -payload "mesaj 3 (alerts, receiver-2 offline)" >> "$OUT/sender.log" 2>&1

step "4. recuperarea: receiver-2 repornește cu același id"
bin/receiver -id receiver-2 -topics news,alerts > "$OUT/receiver-2-after.log" 2>&1 & PIDS+=($!)
sleep 0.5

step "5. validare: schemaVersion 2.0 și topic invalid sunt respinse (INVALID_ARGUMENT)"
bin/sender -topic news -payload "versiune greșită" -schema 2.0 >> "$OUT/sender.log" 2>&1 || true
bin/sender -topic "topic invalid" -payload "x" >> "$OUT/sender.log" 2>&1 || true

step "6. același mesaj în format proto (text) și JSON"
bin/sender -topic news -payload "json demo" -json >> "$OUT/sender.log" 2>&1

step "7. brokerul cade: receiverele reîncearcă de un număr limitat de ori"
kill "$BP"; sleep 0.3
bin/sender -topic news -payload "broker oprit" -timeout 1s >> "$OUT/sender.log" 2>&1 || true
sleep 1

echo
for f in receiver-1 receiver-2-before receiver-2-after; do
  echo "== $f"; grep '^>>>' "$OUT/$f.log" || echo "(nimic)"
done
echo
echo "Log-uri complete în $OUT/"
