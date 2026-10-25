#!/usr/bin/env bash
# Generează un CA local și un certificat de server pentru 127.0.0.1/localhost în certs/.
# Utilizare: ./scripts/certs.sh, apoi
#   broker:          BROKER_TLS_CERT=certs/server.pem BROKER_TLS_KEY=certs/server-key.pem
#   sender/receiver: BROKER_TLS_CA=certs/ca.pem
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p certs && cd certs

openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 365 \
  -keyout ca-key.pem -out ca.pem -subj "/CN=lab2 local CA" 2>/dev/null
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout server-key.pem -out server.csr -subj "/CN=broker" 2>/dev/null
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial -days 365 \
  -extfile <(printf "subjectAltName=IP:127.0.0.1,DNS:localhost\nextendedKeyUsage=serverAuth") \
  -out server.pem 2>/dev/null
rm -f server.csr ca.srl
chmod 600 ca-key.pem server-key.pem
echo "Generat în certs/: $(ls)"
