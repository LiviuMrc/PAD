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
{"time":"2026-10-25T10:06:20.203550992Z","level":"INFO","msg":"receiver offline, mesajele neconfirmate rămân în coadă","component":"broker","event":"receiver.offline","receiver":"receiver-2","queued":0}
```

Senderul publică „mesaj 2” (news) și „mesaj 3” (alerts) și primește `PublishReply` pentru amândouă.
`receiver-1` primește „mesaj 2”. Pentru `receiver-2`, ambele rămân în coadă.

### Recuperarea
`receiver-2` repornește cu **același id**. Brokerul găsește 2 mesaje neconfirmate și le trimite în ordine:

```json
{"time":"2026-10-25T10:06:20.5239905Z","level":"INFO","msg":"receiver abonat","component":"broker","event":"subscribe","result":"ok","correlationId":"ed695084-d71d-4591-b50d-c1e8557fa6d0","receiver":"receiver-2","topics":["alerts","news"],"queued":2,"purged":0,"session":2}
{"time":"2026-10-25T10:06:20.524041958Z","level":"INFO","msg":"mesaj livrat","component":"broker","event":"deliver","result":"ok","receiver":"receiver-2","correlationId":"8a2bcce9-07e5-491f-8e24-4b7bbb92ca01","messageId":"479ccd17-e18c-4b96-bc36-a906fdabd79f"}
{"time":"2026-10-25T10:06:20.524057103Z","level":"INFO","msg":"mesaj livrat","component":"broker","event":"deliver","result":"ok","receiver":"receiver-2","correlationId":"c1e3d733-3217-4fae-a5a2-7597fec52831","messageId":"669ee9ae-e355-49d0-b9ce-796921859fdd"}
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
{"time":"2026-10-25T10:06:21.373686609Z","level":"INFO","msg":"stare refăcută din jurnal","component":"broker","event":"journal.replay","records":21,"receivers":2,"queued":1,"path":"/tmp/tmp.owDz4FWu7B/broker.journal"}
```

`receiver-1` se reconectează singur (`"attempt":1,"code":"Unavailable"`, apoi abonare confirmată).
`receiver-2` repornește și primește „mesaj 4”, publicat înainte de cădere, apoi îl confirmă:

```json
{"time":"2026-10-25T10:06:21.48080483Z","level":"INFO","msg":"receiver abonat","component":"broker","event":"subscribe","result":"ok","correlationId":"242b5a2e-8cb1-457e-a961-5630d0fd8365","receiver":"receiver-2","topics":["alerts","news"],"queued":1,"purged":0,"session":1}
{"time":"2026-10-25T10:06:21.480910982Z","level":"INFO","msg":"mesaj livrat","component":"broker","event":"deliver","result":"ok","receiver":"receiver-2","correlationId":"b74f1489-190c-4d61-8fc6-60f65a0487c6","messageId":"4b64490f-aeb5-46e9-822b-9c31c7dade34"}
{"time":"2026-10-25T10:06:21.483021916Z","level":"INFO","msg":"mesaj confirmat","component":"broker","event":"ack","result":"ok","receiver":"receiver-2","messageId":"4b64490f-aeb5-46e9-822b-9c31c7dade34"}
```

Dacă procesul moare chiar în timpul unei scrieri, ultima linie din jurnal e incompletă. Ea e tăiată la
pornire, pentru că operația respectivă nu a fost confirmată niciodată (`TestTornTailIsTrimmed`).

## Eșecul 3: brokerul se oprește și nu mai revine
La SIGTERM brokerul raportează `NOT_SERVING`, termină apelurile în curs și închide stream-urile cu
`UNAVAILABLE`. Receiverele se reconectează de cel mult `RECONNECT_MAX` ori (în demo 3), apoi se opresc:

```json
{"time":"2026-10-25T10:06:28.054291509Z","level":"ERROR","msg":"renunț la reconectare","component":"receiver","receiver":"receiver-1","event":"reconnect","result":"gave_up","attempts":3}
```

Fiecare încercare chiar contactează brokerul: `common.WaitReady` anulează backoff-ul intern de
reconectare al gRPC, care altfel ar face încercările să eșueze imediat, fără conexiune.
Senderul reîncearcă cu **același** `messageId`; dacă primul apel a ajuns totuși la broker, al doilea
primește `duplicate = true` și mesajul nu e dublat (`TestRetryAfterDeadlineIsDeduplicated`).

Mesajele invalide nu se reîncearcă. Brokerul le respinge cu `INVALID_ARGUMENT`:

```json
{"time":"2026-10-25T10:06:21.024569184Z","level":"WARN","msg":"mesaj respins","component":"broker","event":"message.rejected","result":"rejected","correlationId":"982cef98-df50-4631-8c48-72f53785b0d7","messageId":"aa9a1e75-a699-40c1-ab36-64dd6fa19cc8","error":"schemaVersion nesuportat: \"2.0\" (suportat: 1.0)"}
```

## Garanția obținută
**At-least-once, persistent:** un mesaj confirmat senderului ajunge la fiecare receiver abonat la
topicul lui, în ordinea publicării, chiar dacă receiverul sau brokerul (inclusiv cu `kill -9`) cad
între timp. Limitele: maximum `MAX_QUEUE` mesaje neconfirmate per receiver, iar un receiver offline
mai mult de `RECEIVER_TTL_SEC` e uitat.

## Ce nu acoperă
- **Brokerul oprit definitiv:** nu există replicare. Cât timp e oprit, senderul nu poate publica
  (reîncearcă de `PUBLISH_RETRIES` ori, apoi raportează eroarea).
- **Duplicate după repornirea receiverului:** receiverul ține minte în memorie ce a procesat. Un mesaj
  procesat, dar neconfirmat înainte ca procesul receiver să cadă, e procesat din nou de procesul nou.
- **Receiverul nou:** primește doar mesajele publicate după primul lui `Subscribe`.
- **Doi receiveri cu același id:** ultimul conectat preia coada, iar celălalt primește `CANCELED` și se oprește.
- **Căderea sistemului cu `BROKER_FSYNC=false`:** ultimele operații pot lipsi din jurnal.
