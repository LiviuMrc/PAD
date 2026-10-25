#!/usr/bin/env bash
# Pornește Cassandra și Redis (docker compose) și așteaptă să fie gata.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose up -d
echo -n "aștept Cassandra și Redis"
for _ in $(seq 120); do
  c=$(docker inspect -f '{{.State.Health.Status}}' "$(docker compose ps -q cassandra)")
  r=$(docker inspect -f '{{.State.Health.Status}}' "$(docker compose ps -q redis)")
  if [ "$c" = healthy ] && [ "$r" = healthy ]; then echo " gata"; exit 0; fi
  echo -n "."; sleep 2
done
echo; echo "infrastructura nu a pornit"; docker compose ps; exit 1
