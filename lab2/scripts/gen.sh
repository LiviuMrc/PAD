#!/usr/bin/env bash
# Generează codul Go din contracts/broker.proto în gen/brokerpb/.
# Dacă protoc nu e instalat, descarcă o versiune fixă în .tools/ (fără sudo).
set -euo pipefail
cd "$(dirname "$0")/.."

PROTOC_VERSION=28.3
TOOLS="$PWD/.tools"
mkdir -p "$TOOLS/bin"
export PATH="$TOOLS/bin:$PATH"

if ! command -v protoc >/dev/null; then
  case "$(uname -s)-$(uname -m)" in
    Linux-x86_64)  ARCH=linux-x86_64 ;;
    Linux-aarch64) ARCH=linux-aarch_64 ;;
    Darwin-*)      ARCH=osx-universal_binary ;;
    *) echo "Instalează protoc manual: https://grpc.io/docs/protoc-installation/"; exit 1 ;;
  esac
  echo "Descarc protoc $PROTOC_VERSION ($ARCH) în .tools/"
  curl -sSL -o "$TOOLS/protoc.zip" \
    "https://github.com/protocolbuffers/protobuf/releases/download/v$PROTOC_VERSION/protoc-$PROTOC_VERSION-$ARCH.zip"
  (cd "$TOOLS" && unzip -qo protoc.zip -d . && rm protoc.zip)
fi

# plugin-urile Go, în versiuni fixe
GOBIN="$TOOLS/bin" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.2
GOBIN="$TOOLS/bin" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

mkdir -p gen/brokerpb
protoc -I contracts \
  --go_out=gen/brokerpb --go_opt=paths=source_relative \
  --go-grpc_out=gen/brokerpb --go-grpc_opt=paths=source_relative \
  contracts/broker.proto
echo "Generat: $(ls gen/brokerpb)"
