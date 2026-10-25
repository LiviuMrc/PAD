# Lab 2: Message Broker cu RPC (gRPC + Protocol Buffers)

Este Partea 2 a lucrării „Agent de mesagerie. Invocarea la distanță”. Arhitectura e aceeași ca în
[Lab 1](../lab1/) (sender, broker, 2 receivere, rutare pe topic, coadă per receiver).
Diferența este că socket-urile sunt înlocuite cu **RPC-uri gRPC**, iar XML-ul cu **Protocol Buffers**.

| Cerință (Partea 2) | Unde |
|---|---|
| RPC în loc de socket-uri | serviciul `Broker` din `contracts/broker.proto`: `Publish`, `Ack`, `Unsubscribe` (unary) și `Subscribe` (server streaming) |
| Reprezentarea datelor: proto | `contracts/broker.proto` (pachetul `broker.v1`) |
| Validarea datelor | `common.Validate`. Brokerul răspunde `INVALID_ARGUMENT` |
| Procesare concurentă | gRPC rulează fiecare apel în propria goroutine, iar starea e protejată de un mutex (`TestConcurrentPublishers`) |
| Rutare pe topic + coadă per receiver | `internal/broker/broker.go` |
| JSON | codec gRPC JSON (`common/codec.go`): `-codec json` trimite mesajele pe fir ca JSON (`application/grpc+json`); `sender -json` afișează mesajul în proto text și JSON |

Documentație: [arhitectură + comparație cu Lab 1](docs/architecture.md) ·
[ADR 001](docs/adr/001-grpc-in-loc-de-socketuri.md) ·
[ADR 002](docs/adr/002-livrare-at-least-once-si-persistenta.md) ·
[scenariul de eșec](docs/failure-scenario.md) · log-uri reale în [`docs/evidence/`](docs/evidence/)

## Cerințe

- Go 1.22+
- `curl` și `unzip`. `scripts/gen.sh` descarcă singur `protoc` 28.3 și plugin-urile Go în `.tools/`.
  Dacă ai deja `protoc` instalat, e folosit acela.
- `openssl`, doar pentru `scripts/certs.sh` (TLS)

## Build

Codul generat din `.proto` nu e în repository, deci primul pas e generarea lui:

```bash
cd lab2
./scripts/gen.sh       # creează gen/brokerpb/broker.pb.go și broker_grpc.pb.go
go build ./...
```

## Configurare

Toate variabilele sunt descrise în [`.env.example`](.env.example) și au valori implicite. Programele
citesc fișierul `.env` din directorul curent (`cp .env.example .env`); o variabilă deja setată în
mediu are prioritate față de `.env`, iar flag-urile au prioritate față de amândouă.
Portul implicit e `50061`, iar jurnalul brokerului se scrie în `data/`.

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

**Broker căzut și repornit:** oprește brokerul (chiar și cu `kill -9`), publică ceva cât e oprit
(senderul reîncearcă), pornește-l din nou. Starea se reface din `data/broker.journal`, receiverele se
reconectează singure și primesc mesajele neconfirmate.

**Dezabonare:** `go run ./receiver -id receiver-2 -unsubscribe` șterge abonarea și coada.

**Validare:**
```bash
go run ./sender -topic news -payload x -schema 2.0   # code=InvalidArgument: schemaVersion nesuportat
go run ./sender -topic "nu e valid" -payload x       # code=InvalidArgument: topic invalid
```

**JSON:** `go run ./sender -topic news -payload x -json` (afișare) și
`go run ./sender -topic news -payload x -codec json` (JSON pe fir).

**TLS + token:**
```bash
./scripts/certs.sh
BROKER_TOKEN=secret BROKER_TLS_CERT=certs/server.pem BROKER_TLS_KEY=certs/server-key.pem go run ./broker
BROKER_TOKEN=secret BROKER_TLS_CA=certs/ca.pem go run ./receiver -id receiver-1 -topics news
BROKER_TOKEN=secret BROKER_TLS_CA=certs/ca.pem go run ./sender -topic news -payload "criptat"
```

**Cu un client independent.** `curl` poate face un apel gRPC cu corp JSON (vezi pasul 7 din
`scripts/demo.sh`). Brokerul are reflection activat, deci [grpcurl](https://github.com/fullstorydev/grpcurl)
funcționează fără fișierul `.proto`:
```bash
grpcurl -plaintext 127.0.0.1:50061 list
grpcurl -plaintext -d '{"service":"broker.v1.Broker"}' 127.0.0.1:50061 grpc.health.v1.Health/Check
grpcurl -plaintext -d '{"receiver_id":"r9","topics":["news"]}' 127.0.0.1:50061 broker.v1.Broker/Subscribe
grpcurl -plaintext -d '{"receiver_id":"r9","message_id":"<id>"}' 127.0.0.1:50061 broker.v1.Broker/Ack
```
Cu token: `grpcurl -H 'authorization: Bearer secret' ...`. Un client care nu apelează `Ack`
primește aceleași mesaje din nou la următorul `Subscribe`.

**Oprire:** la SIGTERM/Ctrl+C brokerul raportează `NOT_SERVING`, termină apelurile în curs,
închide stream-urile cu `UNAVAILABLE` și iese cu codul 0. Receiverele reîncearcă de `RECONNECT_MAX`
ori, apoi se opresc.

## Rulare automată

```bash
./scripts/demo.sh      # tot scenariul de mai sus, cu procese reale. Log-urile ajung în docs/evidence/
```

## Teste

```bash
./scripts/gen.sh           # o singură dată, dacă gen/ nu există
go test ./...
go test -race -v ./...
```

Testele pornesc serverul real (`broker.NewServer`, aceeași funcție ca `broker/main.go`) și clienți
gRPC reali. Testele din `broker/` compilează binarul și îl rulează ca proces separat.

| Tip | Test |
|---|---|
| logică locală | `TestNewMessageIsValid`, `TestValidateRejects`, `TestProtoAndJSONRoundTrip`, `TestRecentKeepsLastN`, `TestLoadDotEnv` |
| contract | `TestContractFieldNumbers`, `TestUnknownFieldsAreTolerated`, `TestPublishReplyIsBackwardCompatible` |
| integrare | `TestPublishReachesBothReceivers`, `TestRoutingByTopic`, `TestResubscribeReplacesTopics`, `TestUnsubscribe`, `TestReflectionListsServices` |
| reprezentare | `TestJSONCodecOnTheWire` (octeții reali de pe fir, printr-un proxy TCP), `TestJSONCodecIsRegisteredAndRoundTrips` |
| concurență | `TestConcurrentPublishers` (10 senderi × 20 mesaje în paralel, fără pierderi și fără duplicate) |
| eșec | `TestInvalidMessagesAreRejected`, `TestServerDropsPublishWhoseDeadlineExpired`, `TestSameReceiverIdTakesOver`, `TestSessionEndReason`, `TestCorruptJournalIsAnError` |
| recuperare | `TestOfflineReceiverGetsQueuedMessagesOnReconnect`, `TestUnackedMessageIsRedelivered`, `TestTakeoverRedeliversUnacked`, `TestStateSurvivesRestart`, `TestTornTailIsTrimmed`, `TestCompactionKeepsState` |
| proces real | `TestSIGTERMShutsDownGracefully`, `TestReadsDotEnv`, `TestQueuedMessagesSurviveKill9` |
| clienți | receiver: `TestGivesUpAfterMaxRetries`, `TestReconnectsAcrossBrokerRestarts`, `TestRedeliveryIsProcessedOnce`, ...; sender: `TestRetryAfterDeadlineIsDeduplicated`, `TestRetryUntilBrokerIsUp`, `TestInvalidMessageIsNotRetried` |
| securitate și oprire | `TestTokenAuthentication`, `TestTLS`, `TestHealthGoesNotServingOnShutdown`, `TestShutdownWaitsForInflightPublish`, `TestOfflineReceiverExpires` |

## Contract și semantica erorilor

```protobuf
service Broker {
  rpc Publish(PublishRequest) returns (PublishReply);         // ACK = PublishReply; duplicate = deja acceptat
  rpc Subscribe(SubscribeRequest) returns (stream Message);   // brokerul face push pe stream
  rpc Ack(AckRequest) returns (AckReply);                     // receiverul confirmă procesarea
  rpc Unsubscribe(UnsubscribeRequest) returns (UnsubscribeReply);
}
```

| Cod gRPC | Când | Clientul reîncearcă? |
|---|---|---|
| `OK` | mesaj acceptat. `subscribers` = în câte cozi a intrat; `duplicate = true` dacă `message_id` era deja acceptat | n/a |
| `INVALID_ARGUMENT` | mesajul nu respectă regulile (ID, UTC, `schemaVersion` ≠ 1.0, topic, payload > 4096) | nu |
| `DEADLINE_EXCEEDED` | nu a venit răspuns în `PUBLISH_TIMEOUT_SEC`; un mesaj ajuns după termen nu e pus în cozi | senderul: da, cu același `message_id` |
| `UNAVAILABLE` | brokerul nu rulează sau se oprește | sender și receiver: da, de un număr limitat de ori |
| `UNAUTHENTICATED` | lipsește tokenul sau e greșit | nu |
| `NOT_FOUND` | `Ack`/`Unsubscribe` pentru un receiver necunoscut | nu |
| `CANCELED` | alt receiver s-a abonat cu același id, sau receiverul a fost dezabonat | nu |

Reguli de versionare (scrise și în `.proto`): numerele câmpurilor nu se schimbă, un câmp sau o metodă
nouă primește un număr/nume nou, iar o schimbare incompatibilă înseamnă pachetul `broker.v2`.
`duplicate`, `Ack` și `Unsubscribe` au fost adăugate în v1 fără să strice clienții existenți.

## Garanții și limite

**Garanții:**
- Fiecare mesaj are `message_id`, `correlation_id`, `schema_version` și `occurred_at` în UTC.
  `correlation_id` apare în log-urile sender, broker și receiver.
- Mesajele invalide sunt respinse cu `INVALID_ARGUMENT` și nu ajung la receivere.
- **At-least-once:** un mesaj rămâne în coada receiverului până la `Ack`. Dacă receiverul cade
  înainte de confirmare, mesajul e retrimis, în ordine, la următoarea sesiune.
- **Persistență:** fiecare schimbare e scrisă în `data/broker.journal` și sincronizată pe disc înainte
  de răspuns, deci cozile supraviețuiesc repornirii brokerului, inclusiv după `kill -9`.
- **Deduplicare:** brokerul ține minte ultimele 10000 de `message_id`; senderul reîncearcă sigur.
  Receiverul ignoră retrimiterile deja procesate (după `message_id`).
- Fiecare apel are deadline. Niciun retry nu e infinit: contorul de reconectări al receiverului
  se resetează doar după o abonare confirmată, iar fiecare încercare chiar contactează brokerul.
- Coada are maximum `MAX_QUEUE` (1000) mesaje neconfirmate per receiver. Un receiver offline mai mult
  de `RECEIVER_TTL_SEC` (10 minute) e uitat, cu tot cu coadă.

**Limite:**
- Un singur broker, fără replicare: cât timp e oprit, nimeni nu poate publica (senderul reîncearcă limitat).
- Deduplicarea receiverului e în memorie: după repornirea procesului receiver, un mesaj procesat dar
  neconfirmat poate fi procesat încă o dată (at-least-once, nu exactly-once).
- Tokenul e comun pentru toți clienții: oricine îl are poate folosi orice `receiver_id`.
  Fără TLS, tokenul circulă necriptat (acceptabil doar pe localhost).
- Nu există multicast UDP: e un mecanism de socket, prezentat în Lab 1.

## Versiune

Versiunea curentă: **`lab2-v1.2`**. Tag-urile deja publicate nu se mută, iar corecturile primesc versiuni noi.

| Versiune | Modificări |
|---|---|
| `lab2-v1.2` | `Ack` (at-least-once) și `Unsubscribe`; jurnal pe disc cu compactare; deduplicare `Publish` și reîncercări în sender; abonarea înlocuiește topicurile; TTL pentru receivere offline; brokerul nu mai pune în cozi mesaje cu termenul expirat; motivul corect la închiderea sesiunii; health `NOT_SERVING` real la oprire, apelurile în curs se termină; TLS și token; codec JSON pe fir; `.env` citit efectiv; reîncercările ocolesc backoff-ul intern gRPC; teste pentru sender, receiver și binarul real |
| `lab2-v1.1` | receiverul nu mai raportează abonarea ca reușită când e respinsă, curăță spațiile din `-topics` și resetează contorul doar după o abonare confirmată; gRPC reflection pentru grpcurl |
| `lab2-v1.0` | prima versiune |
