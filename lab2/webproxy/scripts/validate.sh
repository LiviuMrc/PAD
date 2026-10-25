#!/usr/bin/env bash
# Validarea completă: infrastructura reală, go vet, toate testele (cu -race) fără niciun test
# sărit, apoi demo-ul cap-coadă cu procese reale. Orice eșec oprește scriptul.
# Utilizare (din lab2/): ./webproxy/scripts/validate.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
./webproxy/scripts/infra-up.sh
echo "=== go vet"; go vet ./webproxy/...
echo "=== teste (Cassandra și Redis reale, -race)"
out=$(go test -race -count=1 -v ./webproxy/... 2>&1) || { echo "$out" | grep -E '^(---|FAIL|ok)|_test.go'; exit 1; }
echo "$out" | grep -E '^(ok|FAIL)'
if echo "$out" | grep -q -- '--- SKIP'; then echo "teste sărite (infrastructură lipsă):"; echo "$out" | grep -A1 -- '--- SKIP'; exit 1; fi
echo "teste trecute: $(echo "$out" | grep -c -- '--- PASS'), sărite: 0"
echo "=== demo cap-coadă"
./webproxy/scripts/demo.sh | grep -E '^===|OK:|EȘEC|TOATE'
