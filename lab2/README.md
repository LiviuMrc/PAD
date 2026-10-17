# Lab 2: Message Broker cu RPC (gRPC + Protocol Buffers)

Este Partea 2 a lucrării „Agent de mesagerie. Invocarea la distanță”. Arhitectura e aceeași ca în
[Lab 1](../lab1/) (sender, broker, 2 receivere, rutare pe topic, coadă în memorie per receiver).
Diferența este că socket-urile sunt înlocuite cu **RPC-uri gRPC**, iar XML-ul cu **Protocol Buffers**.

| Cerință (Partea 2) | Unde |
|---|---|
| RPC în loc de socket-uri | serviciul `Broker` din `contracts/broker.proto`: `Publish` (unary) și `Subscribe` (server streaming) |
| Reprezentarea datelor: proto | `contracts/broker.proto` (pachetul `broker.v1`) |
| Validarea datelor | `common.Validate`. Brokerul răspunde `INVALID_ARGUMENT` |
| Procesare concurentă | gRPC rulează fiecare apel în propria goroutine, iar starea e protejată de un mutex (`TestConcurrentPublishers`) |
| Rutare pe topic + coadă per receiver | `broker/broker.go` |
| JSON | `sender -json` afișează același mesaj în format proto (text) și în JSON (`protojson`) |

Documentație: [arhitectură + comparație cu Lab 1](docs/architecture.md) ·
[ADR 001](docs/adr/001-grpc-in-loc-de-socketuri.md) ·
[scenariul de eșec](docs/failure-scenario.md) · log-uri reale în [`docs/evidence/`](docs/evidence/)

## Cerințe

- Go 1.22+
- `curl` și `unzip`. `scripts/gen.sh` descarcă singur `protoc` 28.3 și plugin-urile Go în `.tools/`.
  Dacă ai deja `protoc` instalat, e folosit acela.

## Build

Codul generat din `.proto` nu e în repository, deci primul pas e generarea lui:

```bash
cd lab2
./scripts/gen.sh       # creează gen/brokerpb/broker.pb.go și broker_grpc.pb.go
go build ./...
```

Configurarea e în [`.env.example`](.env.example). Toate variabilele au valori implicite.
Portul implicit e `50061`.

## Rulare manuală (demo cu 4 terminale)

```bash
# terminal 1: brokerul
go run ./broker

# terminal 2 și 3: două receivere
go run ./receiver -id receiver-1 -topics news
go run ./receiver -id receiver-2 -topics news,alerts

# terminal 4: senderul
go run ./sender -topic news -payload "salut"        # ajunge la ambele receivere
go run ./sender -topic alerts -payload "alertă"     # ajunge doar la receiver-2
go run ./sender -topic news                         # mod interactiv: o linie = un mesaj
```

**Receiver offline:** oprește receiver-2 (Ctrl+C), publică pe `news`, apoi pornește-l din nou
cu aceeași comandă. Primește imediat mesajele publicate cât a fost oprit.

**Validare:**
```bash
go run ./sender -topic news -payload x -schema 2.0   # code=InvalidArgument: schemaVersion nesuportat
go run ./sender -topic "nu e valid" -payload x       # code=InvalidArgument: topic invalid
```

**JSON:** `go run ./sender -topic news -payload x -json`

**Cu un client gRPC independent ([grpcurl](https://github.com/fullstorydev/grpcurl)).** Brokerul are
reflection activat, deci nu e nevoie de fișierul `.proto`:
```bash
grpcurl -plaintext 127.0.0.1:50061 list
grpcurl -plaintext -d '{"service":"broker.v1.Broker"}' 127.0.0.1:50061 grpc.health.v1.Health/Check
grpcurl -plaintext -d '{"receiver_id":"r9","topics":["news"]}' 127.0.0.1:50061 broker.v1.Broker/Subscribe
```

**Broker căzut:** oprește brokerul. `sender` primește `code=Unavailable`, iar receiverele
reîncearcă de 5 ori, la 2 secunde, apoi se opresc.

## Rulare automată

```bash
./scripts/demo.sh      # tot scenariul de mai sus. Log-urile ajung în docs/evidence/
```

## Teste

```bash
./scripts/gen.sh           # o singură dată, dacă gen/ nu există
go test ./...
go test -race -v ./...
```

| Tip | Test |
|---|---|
| logică locală | `TestNewMessageIsValid`, `TestValidateRejects`, `TestProtoAndJSONRoundTrip` |
| contract | `TestContractFieldNumbers` (numele, numerele câmpurilor și metodele din v1 nu se schimbă), `TestUnknownFieldsAreTolerated` (un câmp nou nu strică consumatorii existenți) |
| integrare | `TestPublishReachesBothReceivers`, `TestRoutingByTopic`, `TestHealth` |
| concurență | `TestConcurrentPublishers` (10 senderi × 20 mesaje în paralel, fără pierderi și fără duplicate) |
| eșec | `TestInvalidMessagesAreRejected`, `TestPublishRespectsDeadline`, `TestSameReceiverIdTakesOver` |
| recuperare | `TestOfflineReceiverGetsQueuedMessagesOnReconnect` |

## Contract și semantica erorilor

```protobuf
service Broker {
  rpc Publish(PublishRequest) returns (PublishReply);       // ACK = PublishReply
  rpc Subscribe(SubscribeRequest) returns (stream Message); // brokerul face push pe stream
}
```

| Cod gRPC | Când | Clientul reîncearcă? |
|---|---|---|
| `OK` | mesaj acceptat. `PublishReply.subscribers` = în câte cozi a intrat | n/a |
| `INVALID_ARGUMENT` | mesajul nu respectă regulile (ID, UTC, `schemaVersion` ≠ 1.0, topic, payload > 4096) | nu |
| `DEADLINE_EXCEEDED` | nu a venit răspuns în `PUBLISH_TIMEOUT_SEC` (implicit 3s) | nu automat. Retrimiterea manuală poate duplica |
| `UNAVAILABLE` | brokerul nu rulează | receiverul: da, de maximum `RECONNECT_MAX` ori |
| `CANCELED` | alt receiver s-a abonat cu același id | nu |

Reguli de versionare (scrise și în `.proto`): numerele câmpurilor nu se schimbă, un câmp nou primește
un număr nou, iar o schimbare incompatibilă înseamnă pachetul `broker.v2`.

## Garanții și limite

**Garanții:**
- Fiecare mesaj are `message_id`, `correlation_id`, `schema_version` și `occurred_at` în UTC.
  `correlation_id` apare în log-urile sender, broker și receiver.
- Mesajele invalide sunt respinse cu `INVALID_ARGUMENT` și nu ajung la receivere.
- Un receiver offline primește la reconectare, în ordine, mesajele din coadă, cât timp
  brokerul nu a fost repornit. Coada are maximum 1000 de mesaje per receiver.
- Fiecare apel are deadline. Niciun retry nu e infinit: contorul de reconectări al receiverului
  se resetează doar după o abonare confirmată, iar la `INVALID_ARGUMENT` sau `CANCELED`
  receiverul se oprește.

**Limite:**
- Nu există persistență: la repornirea brokerului se pierd cozile.
- Receiverul nu confirmă mesajele. Un mesaj aflat „în zbor” când receiverul cade se poate pierde
  (best-effort, deci nici at-least-once, nici exactly-once).
- Nu există deduplicare, autentificare sau TLS (conexiunile gRPC sunt `insecure`, doar pentru localhost).
- Nu există multicast UDP: e un mecanism de socket, prezentat în Lab 1.

## Versiune

Versiunea curentă: **`lab2-v1.1`**. Tag-urile deja publicate nu se mută, iar corecturile primesc versiuni noi.

| Versiune | Modificări |
|---|---|
| `lab2-v1.1` | receiverul nu mai raportează abonarea ca reușită când e respinsă, curăță spațiile din `-topics` și resetează contorul doar după o abonare confirmată; gRPC reflection pentru grpcurl |
| `lab2-v1.0` | prima versiune |
