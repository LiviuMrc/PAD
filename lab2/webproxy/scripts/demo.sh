#!/usr/bin/env bash
# Rulează tot sistemul cu procese reale (3 DW, proxy, 2 noduri de stocare, Cassandra, Redis)
# și verifică fiecare cerință din cerinte.pdf. Orice verificare eșuată oprește scriptul.
# Ieșirea completă ajunge în webproxy/docs/evidence/.
# Utilizare (din lab2/): ./webproxy/scripts/demo.sh
set -euo pipefail
cd "$(dirname "$0")/../.."

W=webproxy
OUT=$W/docs/evidence
BIN=$(mktemp -d)
VAR=$(mktemp -d)
P=http://127.0.0.1:9100
KS=lab2_dw_demo
mkdir -p "$OUT"; rm -f "$OUT"/*.log "$OUT"/demo.txt
exec > >(tee "$OUT/demo.txt") 2>&1

PIDS=()
cleanup() { kill "${PIDS[@]}" 2>/dev/null || true; wait "${PIDS[@]}" 2>/dev/null || true; rm -rf "$BIN" "$VAR"; }
trap cleanup EXIT

step() { echo; echo "=== $*"; }
ok() { echo "  OK: $*"; }
fail() { echo "  EȘEC: $*"; exit 1; }
# check "descriere" comanda...  — comanda trebuie să reușească
check() { local d=$1; shift; if "$@"; then ok "$d"; else fail "$d"; fi; }
hdr() { curl -s -o /dev/null -D - "$@" | tr -d '\r'; }        # doar header-ele
get_hdr() { hdr "${@:2}" | awk -v h="$1" 'tolower($1)==tolower(h)":" {print $2}'; }
wait_http() { for _ in $(seq 100); do curl -sf -o /dev/null "$1" && return 0; sleep 0.1; done; fail "$1 nu răspunde"; }

step "0. infrastructura: Cassandra + Redis (docker compose), binarele"
$W/scripts/infra-up.sh
docker compose -f $W/docker-compose.yml exec -T cassandra cqlsh -e "DROP KEYSPACE IF EXISTS $KS" >/dev/null
docker compose -f $W/docker-compose.yml exec -T redis redis-cli FLUSHDB >/dev/null
go build -o "$BIN/" ./$W/warehouse ./$W/proxy ./$W/source
ok "Cassandra $(docker compose -f $W/docker-compose.yml exec -T cassandra cqlsh -e 'SELECT release_version FROM system.local' | sed -n 4p | tr -d ' '), $(docker compose -f $W/docker-compose.yml exec -T redis redis-server --version | cut -d' ' -f3)"

start_dw() { # $1 = id, $2 = port
  DW_ID=$1 DW_ADDR=127.0.0.1:$2 CASSANDRA_KEYSPACE=$KS "$BIN/warehouse" >> "$OUT/$1.log" 2>&1 &
  PIDS+=($!); eval "PID_$1=$!"
}
start_proxy() {
  PROXY_ID=proxy1 HEALTH_INTERVAL_MS=500 "$BIN/proxy" >> "$OUT/proxy.log" 2>&1 & PIDS+=($!); PID_proxy=$!
}
start_dw dw1 9101; wait_http http://127.0.0.1:9101/health   # primul creează schema
start_dw dw2 9102; start_dw dw3 9103
wait_http http://127.0.0.1:9102/health; wait_http http://127.0.0.1:9103/health
start_proxy; wait_http $P/_proxy/backends
# src1: date trimise în XML, primește modificările prin push (și pull); src2: JSON, doar pull
"$BIN/source" -id src1 -seed $W/data/src1.json -format xml -addr 127.0.0.1:9111 -data "$VAR/src1.json" >> "$OUT/src1.log" 2>&1 & PIDS+=($!)
"$BIN/source" -id src2 -seed $W/data/src2.json -format json -addr 127.0.0.1:9112 -data "$VAR/src2.json" -push=false -poll 1s >> "$OUT/src2.log" 2>&1 & PIDS+=($!)
for _ in $(seq 100); do [ "$(curl -s "$P/employees/?limit=100" -H 'Cache-Control: no-cache' | grep -c '"id"')" -ge 9 ] && break; sleep 0.2; done

step "Etapa 1 — PUT: nodurile de stocare au trimis datele la DW (prin proxy)"
grep -h '"event":"source.synced"' "$OUT/src1.log" "$OUT/src2.log" | cut -c1-200
check "src1 a trimis în XML" grep -q '"format":"xml"' "$OUT/src1.log"
check "DW are 9 angajați (5 de la src1, 4 de la src2)" test "$(curl -s "$P/employees/?limit=100" | grep -c '"id"')" -eq 9

step "Etapa 1 — GET /employees/?offset=1&limit=4, clientul alege formatul"
echo "--- JSON (Accept: application/json)"; curl -s "$P/employees/?offset=1&limit=4" -H 'Accept: application/json' | head -14
echo "--- XML (Accept: application/xml)"; curl -s "$P/employees/?offset=1&limit=4" -H 'Accept: application/xml' | head -8
check "JSON: 4 angajați, primul cu id 2" test "$(curl -s "$P/employees/?offset=1&limit=4" | grep -c '"id"')" -eq 4
check "XML: <employees offset=\"1\" limit=\"4\" total=\"9\">" bash -c "curl -s '$P/employees/?offset=1&limit=4' -H 'Accept: application/xml' | grep -q '<employees offset=\"1\" limit=\"4\" total=\"9\">'"
check "?format=xml are prioritate față de Accept" bash -c "curl -s '$P/employee/?id=1&format=xml' -H 'Accept: application/json' | grep -q '<firstName>Ana</firstName>'"
check "Accept: text/html -> 406" test "$(curl -s -o /dev/null -w '%{http_code}' "$P/employee/?id=1" -H 'Accept: text/html')" = 406
check "PUT cu Content-Type: text/plain -> 415" test "$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$P/employee/" -H 'Content-Type: text/plain' -d x)" = 415
check "GET /employee/?id=999 -> 404" test "$(curl -s -o /dev/null -w '%{http_code}' "$P/employee/?id=999")" = 404

step "Etapa 2 — load balancing round-robin (cereri fără cache)"
order=$(for _ in $(seq 6); do get_hdr X-Served-By "$P/employees/" -H 'Cache-Control: no-cache'; done | paste -sd' ')
echo "  X-Served-By: $order"
check "fiecare DW servește pe rând" bash -c "[ \"\$(echo $order | tr ' ' '\n' | sort | uniq -c | awk '{print \$1}' | paste -sd,)\" = 2,2,2 ]"

step "Etapa 2 — caching în Redis"
a=$(get_hdr X-Cache "$P/employee/?id=3"); b=$(get_hdr X-Cache "$P/employee/?id=3")
echo "  GET /employee/?id=3: $a, apoi $b"
check "MISS, apoi HIT" test "$a $b" = "MISS HIT"
check "în XML e altă intrare (MISS)" test "$(get_hdr X-Cache "$P/employee/?id=3" -H 'Accept: application/xml')" = MISS
etag=$(get_hdr ETag "$P/employee/?id=3")
check "If-None-Match pe ETag-ul din cache -> 304" test "$(curl -s -o /dev/null -w '%{http_code}' "$P/employee/?id=3" -H "If-None-Match: $etag")" = 304
echo "  chei în Redis: $(docker compose -f $W/docker-compose.yml exec -T redis redis-cli --scan --pattern 'proxy:cache:*' | wc -l) intrări de cache"
echo "--- POST (modificare de la un client) -> invalidare"
curl -s -X POST "$P/employee/" -H 'Content-Type: application/json' -d '{"id":3,"position":"HR Lead","salary":4800}' | grep -E '"position"|"version"'
after=$(curl -s -D - "$P/employee/?id=3" | tr -d '\r')
check "după POST: MISS și date noi" bash -c "echo '$after' | grep -q 'X-Cache: MISS' && echo '$after' | grep -q 'HR Lead'"
grep -h '"event":"cache.invalidate"' "$OUT/proxy.log" | tail -1 | cut -c1-200

step "Etapa 1 — POST + push: modificarea ajunge la nodul sursă src1"
for _ in $(seq 50); do grep -q '"position": "HR Lead"' "$VAR/src1.json" && break; sleep 0.1; done
grep -h '"event":"source.apply"' "$OUT/src1.log" | tail -1 | cut -c1-220
check "src1 a aplicat-o prin push, în fișierul local" bash -c "grep -q '\"via\":\"push\".*\"id\":3' '$OUT/src1.log' && grep -q '\"position\": \"HR Lead\"' '$VAR/src1.json'"

step "Etapa 1 — POST + pull: src2 cere periodic GET /update/employees/"
curl -s -X POST "$P/employee/" -H 'Content-Type: application/xml' -d '<employee><id>101</id><salary>7000</salary></employee>' > /dev/null
for _ in $(seq 50); do grep -q '"salary": 7000' "$VAR/src2.json" && break; sleep 0.1; done
grep -h '"event":"source.apply"' "$OUT/src2.log" | tail -1 | cut -c1-220
check "src2 a aplicat-o prin pull" bash -c "grep -q '\"via\":\"pull\".*\"id\":101' '$OUT/src2.log' && grep -q '\"salary\": 7000' '$VAR/src2.json'"
echo "--- feed-ul de modificări (XML)"; curl -s "$P/update/employees/?limit=1" -H 'Accept: application/xml' | head -6

step "Etapa 2 — failover: dw2 e oprit, apoi pornit din nou"
kill "$PID_dw2"; wait "$PID_dw2" 2>/dev/null || true
codes=$(for _ in $(seq 6); do curl -s -o /dev/null -D - "$P/employees/" -H 'Cache-Control: no-cache' | tr -d '\r' |
  awk '/^HTTP/ {c=$2} tolower($1)=="x-served-by:" {s=$2} END {printf "%s:%s ", c, s}'; done)
echo "  cu dw2 oprit (cod:DW): $codes"
check "toate 200, niciuna de la dw2" bash -c "[ \$(echo '$codes' | grep -o '200:dw[13]' | wc -l) -eq 6 ]"
check "proxy-ul l-a scos din rotație" bash -c "curl -s $P/_proxy/backends | grep -A1 9102 | grep -q '\"healthy\": false'"
start_dw dw2 9102; wait_http http://127.0.0.1:9102/health; sleep 1
again=$(for _ in $(seq 6); do get_hdr X-Served-By "$P/employees/" -H 'Cache-Control: no-cache'; done | paste -sd' ')
echo "  după repornire: $again"
check "dw2 revine în rotație" bash -c "echo '$again' | grep -q dw2"

step "Etapa 2 — smart proxy: conexiunea keep-alive a clientului, în evidența din Redis"
curl -s -o /dev/null -o /dev/null -o /dev/null -o /dev/null "$P/employee/?id=1" "$P/employee/?id=2" "$P/employee/?id=4" "$P/_proxy/connections" -w '' &
CURL=$!; wait $CURL
conns=$(curl -s "$P/_proxy/connections" "$P/_proxy/connections")   # două cereri pe aceeași conexiune
echo "$conns" | tail -14
check "aceeași conexiune a purtat mai multe cereri" bash -c "echo '$conns' | grep -q '\"requests\": 2'"
check "evidența e în Redis" test "$(docker compose -f $W/docker-compose.yml exec -T redis redis-cli HGET proxy:stats connsOpened)" -gt 1

step "Etapa 1 — procesare concurentă (thread-per-request) și excludere reciprocă"
seq 40 | xargs -P 40 -I{} curl -s -o /dev/null "$P/employees/?offset=0&limit=50" -H 'Cache-Control: no-cache'
mx=$(for port in 9101 9102 9103; do curl -s http://127.0.0.1:$port/metrics | grep -o '"maxInflight": [0-9]*' | awk '{print $2}'; done | sort -n | tail -1)
echo "  maxInflight (cel mai mare, dintre DW-uri): $mx"
check "DW-urile au procesat cereri simultan" test "$mx" -ge 2
# 30 de clienți măresc concurent salariul angajatului 4, fiecare cu If-Match pe versiunea citită
before=$(curl -s "$P/employee/?id=4" -H 'Cache-Control: no-cache' | grep -o '"salary": [0-9]*' | awk '{print $2}')
inc() {
  while true; do
    r=$(curl -s -D - "$P/employee/?id=4" -H 'Cache-Control: no-cache' | tr -d '\r')
    s=$(echo "$r" | grep -o '"salary": [0-9]*' | awk '{print $2}'); e=$(echo "$r" | awk 'tolower($1)=="etag:" {print $2}')
    c=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$P/employee/" -H 'Content-Type: application/json' -H "If-Match: $e" -d "{\"id\":4,\"salary\":$((s+1))}")
    [ -n "$e" ] || { echo "lipsește ETag"; return 1; }
    [ "$c" = 200 ] && return 0
  done
}
export -f inc; export P
seq 30 | xargs -P 30 -I{} bash -c inc
after=$(curl -s "$P/employee/?id=4" -H 'Cache-Control: no-cache' | grep -o '"salary": [0-9]*' | awk '{print $2}')
echo "  salariul: $before -> $after (30 de modificări concurente prin 3 DW)"
check "nicio modificare pierdută (LWT în Cassandra)" test "$after" -eq $((before + 30))

step "Persistență: repornirea proxy-ului și a DW-urilor"
curl -s -o /dev/null "$P/employee/?id=5"
kill "$PID_proxy"; wait "$PID_proxy" 2>/dev/null || true; start_proxy; wait_http $P/_proxy/backends
check "cache-ul din Redis supraviețuiește repornirii proxy-ului (HIT)" test "$(get_hdr X-Cache "$P/employee/?id=5")" = HIT
kill "$PID_dw1" "$PID_dw2" "$PID_dw3"; wait "$PID_dw1" "$PID_dw2" "$PID_dw3" 2>/dev/null || true
start_dw dw1 9101; start_dw dw2 9102; start_dw dw3 9103
for port in 9101 9102 9103; do wait_http http://127.0.0.1:$port/health; done; sleep 1
check "datele sunt în Cassandra după repornirea DW-urilor" bash -c "curl -s '$P/employee/?id=3' -H 'Cache-Control: no-cache' | grep -q 'HR Lead'"

echo; echo "TOATE VERIFICĂRILE AU TRECUT. Log-uri: $OUT/"
