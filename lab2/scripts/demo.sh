#!/usr/bin/env bash
# Rulează scenariul complet cu procese reale și salvează log-urile în docs/evidence/.
# Utilizare: ./scripts/demo.sh   (rulează întâi ./scripts/gen.sh)
set -euo pipefail
cd "$(dirname "$0")/.."

export BROKER_ADDR="${BROKER_ADDR:-127.0.0.1:50061}"
TLS_ADDR=127.0.0.1:50062
OUT=docs/evidence
DATA=$(mktemp -d)
mkdir -p bin "$OUT"
rm -f "$OUT"/*.log
go build -o bin/ ./broker ./sender ./receiver
# receiverele demo-ului: 3 reconectări, câte 0.5s, ca scenariul să dureze puțin
RECV="-retries 3 -interval 500ms -connect-timeout 500ms"

PIDS=()
cleanup() { kill "${PIDS[@]}" 2>/dev/null || true; rm -rf bin "$DATA"; }
trap cleanup EXIT

step() { echo; echo "### $*" | tee -a "$OUT/sender.log"; }
send() { bin/sender "$@" >> "$OUT/sender.log" 2>&1 || true; }
start_broker() { # $1 = fișierul de log
  BROKER_DATA_DIR="$DATA" bin/broker >> "$OUT/$1" 2>&1 & BP=$!; PIDS+=($BP)
  for _ in $(seq 50); do grep -q broker.started "$OUT/$1" 2>/dev/null && return; sleep 0.1; done
  echo "Brokerul nu a pornit:"; cat "$OUT/$1"; exit 1
}

start_broker broker.log

# starea inițială: 2 receivere online
bin/receiver -id receiver-1 -topics news $RECV > "$OUT/receiver-1.log" 2>&1 & R1=$!; PIDS+=($R1)
bin/receiver -id receiver-2 -topics news,alerts $RECV > "$OUT/receiver-2-before.log" 2>&1 & R2=$!; PIDS+=($R2)
sleep 0.5

step "1. Publish cu ambele receivere online"
send -topic news -payload "mesaj 1 (ambii online)"

step "2. injectarea eșecului: receiver-2 este oprit"
kill "$R2"; sleep 0.3

step "3. Publish cât timp receiver-2 e offline"
send -topic news -payload "mesaj 2 (receiver-2 offline)"
send -topic alerts -payload "mesaj 3 (alerts, receiver-2 offline)"

step "4. recuperarea: receiver-2 repornește cu același id și primește mesajele neconfirmate"
bin/receiver -id receiver-2 -topics news,alerts $RECV > "$OUT/receiver-2-after.log" 2>&1 & R2=$!; PIDS+=($R2)
sleep 0.5

step "5. validare: schemaVersion 2.0 și topic invalid sunt respinse (INVALID_ARGUMENT, fără reîncercări)"
send -topic news -payload "versiune greșită" -schema 2.0
send -topic "topic invalid" -payload "x"

step "6. același mesaj în format proto (text) și JSON; apoi trimis pe fir ca JSON (-codec json)"
send -topic news -payload "json demo" -json
send -topic news -payload "trimis pe fir ca JSON" -codec json

step "7. client independent: curl face un apel gRPC cu corp JSON (application/grpc+json)"
body=$(printf '{"message":{"messageId":"%s","correlationId":"%s","schemaVersion":"1.0","occurredAt":"%s","sender":"curl","topic":"news","payload":"trimis cu curl"}}' \
  "$(cat /proc/sys/kernel/random/uuid)" "$(cat /proc/sys/kernel/random/uuid)" "$(date -u +%Y-%m-%dT%H:%M:%SZ)")
{
  echo "\$ curl --http2-prior-knowledge -H 'content-type: application/grpc+json' http://$BROKER_ADDR/broker.v1.Broker/Publish"
  echo "cerere: $body"
  printf 'răspuns: '
  { printf '\x00'; printf "%08x" ${#body} | sed 's/../\\x&/g' | xargs -0 printf; printf '%s' "$body"; } |
    curl -s --http2-prior-knowledge -H 'content-type: application/grpc+json' -H 'te: trailers' \
      --data-binary @- -D "$OUT/curl-headers.tmp" "http://$BROKER_ADDR/broker.v1.Broker/Publish" | tail -c +6
  echo; tr -d '\r' < "$OUT/curl-headers.tmp" | grep -E '^(HTTP|content-type|grpc-status)'
} >> "$OUT/sender.log" 2>&1
rm -f "$OUT/curl-headers.tmp"

step "8. kill -9 pe broker cu receiver-2 offline: mesajul confirmat e deja în jurnal"
kill "$R2"; sleep 0.3
send -topic alerts -payload "mesaj 4 (înainte de kill -9)"
kill -9 "$BP"; wait "$BP" 2>/dev/null || true
echo "brokerul a fost omorât cu SIGKILL" >> "$OUT/sender.log"
start_broker broker-after-kill9.log
bin/receiver -id receiver-2 -topics news,alerts $RECV > "$OUT/receiver-2-after-kill9.log" 2>&1 & R2=$!; PIDS+=($R2)
sleep 1.5   # receiver-1 se reconectează singur

step "9. dezabonare: receiver-2 iese, iar coada lui e ștearsă (news mai are un singur abonat)"
kill "$R2"; sleep 0.3
bin/receiver -id receiver-2 -unsubscribe >> "$OUT/sender.log" 2>&1
send -topic news -payload "mesaj 5 (după dezabonarea receiver-2)"

step "10. TLS + token: un broker separat pe $TLS_ADDR"
./scripts/certs.sh > /dev/null
BROKER_ADDR=$TLS_ADDR BROKER_DATA_DIR= BROKER_TOKEN=secret BROKER_TLS_CERT=certs/server.pem BROKER_TLS_KEY=certs/server-key.pem \
  bin/broker > "$OUT/broker-tls.log" 2>&1 & PIDS+=($!)
sleep 0.5
echo "-- cu CA și token corect:" >> "$OUT/sender.log"
BROKER_TOKEN=secret send -addr $TLS_ADDR -tls-ca certs/ca.pem -topic news -payload "criptat și autentificat"
echo "-- fără token:" >> "$OUT/sender.log"
send -addr $TLS_ADDR -tls-ca certs/ca.pem -topic news -payload "x" -retries 0
echo "-- fără TLS:" >> "$OUT/sender.log"
BROKER_TOKEN=secret send -addr $TLS_ADDR -topic news -payload "x" -retries 0 -timeout 1s

step "11. brokerul se oprește (SIGTERM): receiverele reîncearcă de 3 ori, apoi renunță"
kill -TERM "$BP"
wait "$R1" || echo "receiver-1 s-a oprit cu codul $?" >> "$OUT/sender.log"
send -topic news -payload "broker oprit" -timeout 500ms -retries 1 -retry-interval 200ms

echo
for f in receiver-1 receiver-2-before receiver-2-after receiver-2-after-kill9; do
  echo "== $f"; grep '^>>>' "$OUT/$f.log" || echo "(nimic)"
done
echo
grep -h '"gave_up"' "$OUT/receiver-1.log" | cut -c1-160
echo
echo "Log-uri complete în $OUT/"
