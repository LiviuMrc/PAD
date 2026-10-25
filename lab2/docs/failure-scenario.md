# Scenariul de eșec: receiver offline, broker omorât, reconectare (gRPC)

Reproducere automată:
`go test ./internal/broker -run 'Offline|Unacked|Takeover|Restart' -v` și
`go test ./broker -run Kill9 -v` (binarul real, oprit cu SIGKILL).
Reproducere cu procese reale: `./scripts/demo.sh`. Log-urile se scriu în `docs/evidence/`.

## Starea inițială
Brokerul gRPC rulează pe `127.0.0.1:50061`, cu jurnalul în `BROKER_DATA_DIR`. `receiver-1` are un
stream `Subscribe` deschis pe `news`, iar `receiver-2` pe `news` și `alerts`. Ambele primesc și confirmă „mesaj 1”.

## Eșecul 1: receiverul cade
Procesul `receiver-2` e oprit (`kill`). Contextul stream-ului se anulează pe server, iar brokerul
marchează receiverul offline. Mesajele lui rămân în coadă până la `Ack`:

```json
{"time":"2026-10-25T10:26:44.385610844Z","level":"INFO","msg":"receiver offline, mesajele neconfirmate rămân în coadă","component":"broker","event":"receiver.offline","receiver":"receiver-2","queued":0}
```

Senderul publică „mesaj 2” (news) și „mesaj 3” (alerts) și primește `PublishReply` pentru amândouă.
`receiver-1` primește „mesaj 2”. Pentru `receiver-2`, ambele rămân în coadă.

### Recuperarea
`receiver-2` repornește cu **același id**. Brokerul găsește 2 mesaje neconfirmate și le trimite în ordine:

```json
{"time":"2026-10-25T10:26:44.70680756Z","level":"INFO","msg":"receiver abonat","component":"broker","event":"subscribe","result":"ok","correlationId":"bc7a73dd-c649-4c95-b6d3-f71b4541206b","receiver":"receiver-2","topics":["alerts","news"],"queued":2,"purged":0,"session":2}
{"time":"2026-10-25T10:26:44.706944772Z","level":"INFO","msg":"mesaj livrat","component":"broker","event":"deliver","result":"ok","receiver":"receiver-2","correlationId":"51a92965-6476-4d12-ba56-d212decefcea","messageId":"96bcc3f5-7db6-48a2-a827-77998d4ac95b"}
{"time":"2026-10-25T10:26:44.706967597Z","level":"INFO","msg":"mesaj livrat","component":"broker","event":"deliver","result":"ok","receiver":"receiver-2","correlationId":"e93ddaf6-b808-4358-a16e-70289df08aef","messageId":"7cfb43ca-83d4-4948-9b11-526cf14a43a5"}
```

Un mesaj trimis pe stream, dar neconfirmat (receiverul a căzut înainte de `Ack`), e retrimis la
următoarea sesiune (`TestUnackedMessageIsRedelivered`). La fel și când o sesiune e preluată de alta cu
același id (`TestTakeoverRedeliversUnacked`). Receiverul recunoaște retrimiterile deja procesate după
`messageId` și nu le procesează de două ori (`TestRedeliveryIsProcessedOnce`).

## Eșecul 2: brokerul e omorât cu `kill -9`
`receiver-2` e din nou offline. Senderul publică „mesaj 4” pe `alerts` și primește confirmarea, ceea ce
înseamnă că mesajul e deja în jurnal, sincronizat pe disc. Brokerul e omorât cu SIGKILL, fără nicio
șansă de curățenie, apoi e pornit din nou și reface starea din jurnal:

```json
{"time":"2026-10-25T10:26:45.556173601Z","level":"INFO","msg":"stare refăcută din jurnal","component":"broker","event":"journal.replay","records":21,"receivers":2,"queued":1,"deadLetters":0,"path":"/tmp/tmp.kmhnWiscZk/broker.journal"}
```

`receiver-1` se reconectează singur (`"attempt":1,"code":"Unavailable"`, apoi abonare confirmată).
`receiver-2` repornește și primește „mesaj 4”, publicat înainte de cădere, apoi îl confirmă:

```json
{"time":"2026-10-25T10:26:45.664481633Z","level":"INFO","msg":"receiver abonat","component":"broker","event":"subscribe","result":"ok","correlationId":"c330eb74-a650-45bc-af9f-b12d73b8b8a9","receiver":"receiver-2","topics":["alerts","news"],"queued":1,"purged":0,"session":1}
{"time":"2026-10-25T10:26:45.664608493Z","level":"INFO","msg":"mesaj livrat","component":"broker","event":"deliver","result":"ok","receiver":"receiver-2","correlationId":"90afc2a4-508f-467a-9953-73ddc852f44c","messageId":"9269295f-d542-44b8-b97f-513cb5cff663"}
{"time":"2026-10-25T10:26:45.666279604Z","level":"INFO","msg":"mesaj confirmat","component":"broker","event":"ack","result":"ok","receiver":"receiver-2","messageId":"9269295f-d542-44b8-b97f-513cb5cff663"}
```

Dacă procesul moare chiar în timpul unei scrieri, ultima linie din jurnal e incompletă. Ea e tăiată la
pornire, pentru că operația respectivă nu a fost confirmată niciodată (`TestTornTailIsTrimmed`).

## Eșecul 3: brokerul se oprește și nu mai revine
La SIGTERM brokerul raportează `NOT_SERVING`, termină apelurile în curs și închide stream-urile cu
`UNAVAILABLE`. Receiverele se reconectează de cel mult `RECONNECT_MAX` ori (în demo 3), apoi se opresc:

```json
{"time":"2026-10-25T10:26:54.323415503Z","level":"ERROR","msg":"renunț la reconectare","component":"receiver","receiver":"receiver-1","event":"reconnect","result":"gave_up","attempts":3}
```

Fiecare încercare chiar contactează brokerul: `common.WaitReady` anulează backoff-ul intern de
reconectare al gRPC, care altfel ar face încercările să eșueze imediat, fără conexiune.
Senderul reîncearcă cu **același** `messageId`; dacă primul apel a ajuns totuși la broker, al doilea
primește `duplicate = true` și mesajul nu e dublat (`TestRetryAfterDeadlineIsDeduplicated`).

Mesajele invalide nu se reîncearcă. Brokerul le respinge cu `INVALID_ARGUMENT`:

```json
{"time":"2026-10-25T10:26:45.209087843Z","level":"WARN","msg":"mesaj respins","component":"broker","event":"message.rejected","result":"rejected","correlationId":"092f1777-8158-4b11-ab6a-6561cb7674dd","messageId":"d9b16848-781a-4817-9b26-d7ff9785e7ed","error":"schemaVersion nesuportat: \"2.0\" (suportat: 1.0)"}
```

## Garanția obținută
**At-least-once, persistent:** un mesaj confirmat senderului ajunge la fiecare receiver abonat la
topicul lui, în ordinea publicării, chiar dacă receiverul sau brokerul (inclusiv cu `kill -9`) cad
între timp. Când coada unui receiver depășește `MAX_QUEUE` sau receiverul stă offline mai mult de
`RECEIVER_TTL_SEC`, mesajele nu se pierd: trec în DLQ, de unde pot fi retrimise cu
`go run ./dlq -redrive -receiver <id>` (pasul 10 din `scripts/demo.sh`, inclusiv după `kill -9`).

## Ce nu acoperă
- **Brokerul oprit definitiv:** nu există replicare. Cât timp e oprit, senderul nu poate publica
  (reîncearcă de `PUBLISH_RETRIES` ori, apoi raportează eroarea).
- **Duplicate după repornirea receiverului:** receiverul ține minte în memorie ce a procesat. Un mesaj
  procesat, dar neconfirmat înainte ca procesul receiver să cadă, e procesat din nou de procesul nou.
- **Receiverul nou:** primește doar mesajele publicate după primul lui `Subscribe`.
- **Doi receiveri cu același id:** ultimul conectat preia coada, iar celălalt primește `CANCELED` și se oprește.
- **Căderea sistemului cu `BROKER_FSYNC=false`:** ultimele operații pot lipsi din jurnal.
- **DLQ plină:** peste `DLQ_MAX` mesaje, cel mai vechi din DLQ e eliminat definitiv (log `ERROR`).
