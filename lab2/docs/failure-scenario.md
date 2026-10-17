# Scenariul de eșec: receiver offline, apoi reconectat (gRPC)

Reproducere automată: `go test ./broker -run Offline -v`
(test `TestOfflineReceiverGetsQueuedMessagesOnReconnect`)
Reproducere cu procese reale: `./scripts/demo.sh`. Log-urile se scriu în `docs/evidence/`.

## Starea inițială
Brokerul gRPC rulează pe `127.0.0.1:50061`. `receiver-1` are un stream `Subscribe` deschis pe `news`,
iar `receiver-2` pe `news` și `alerts`. Ambele primesc „mesaj 1”.

## Injectarea eșecului
Procesul `receiver-2` e oprit (`kill`). Contextul stream-ului se anulează pe server, iar brokerul
marchează receiverul offline:

```json
{"time":"2026-10-17T15:50:38.383515274Z","level":"INFO","msg":"receiver deconectat, mesajele noi rămân în coadă","component":"broker","event":"receiver.offline","receiver":"receiver-2","queued":0}
```

## Comportamentul observat
Senderul apelează `Publish` pentru „mesaj 2” (news) și „mesaj 3” (alerts) și primește `PublishReply`
pentru amândouă. `receiver-1` primește „mesaj 2”. Mesajele pentru `receiver-2` rămân în coada lui.

## Mecanismul de recuperare
`receiver-2` repornește cu **același id** și deschide un nou `Subscribe`. Brokerul găsește 2 mesaje
în coadă și le trimite pe stream, în ordine:

```json
{"time":"2026-10-17T15:50:38.701221748Z","level":"INFO","msg":"receiver abonat","component":"broker","event":"subscribe","result":"ok","correlationId":"d8b9309c-f76e-4596-b66d-43a48f22fbae","receiver":"receiver-2","topics":["news","alerts"],"queued":2}
{"time":"2026-10-17T15:50:38.701296265Z","level":"INFO","msg":"mesaj livrat","component":"broker","event":"deliver","result":"ok","receiver":"receiver-2","correlationId":"8c90ec5d-a7ea-4ef7-a0da-0f58bee537ba","messageId":"b5f2e743-5a51-49ec-a391-7f9048c0fe4d"}
{"time":"2026-10-17T15:50:38.701300994Z","level":"INFO","msg":"mesaj livrat","component":"broker","event":"deliver","result":"ok","receiver":"receiver-2","correlationId":"61c34eef-2423-468c-bb70-5cebc986feb3","messageId":"d0e2275a-adf8-4d80-a163-7f2d8f2df45a"}
```

Dacă `stream.Send` eșuează, mesajul e pus înapoi în fața cozii și va fi livrat la următoarea reconectare.

Dacă brokerul e cel care cade, `Publish` întoarce `UNAVAILABLE`, iar receiverele se reconectează
de cel mult `RECONNECT_MAX` ori (implicit 5), la `RECONNECT_INTERVAL_SEC` secunde, apoi se opresc:

```json
{"msg":"conexiune pierdută, reîncerc","component":"receiver","receiver":"receiver-1","event":"reconnect","attempt":1,"max":5,"in":"2s","code":"Unavailable"}
```

Mesajele invalide nu se reîncearcă. Brokerul le respinge cu `INVALID_ARGUMENT`:

```json
{"level":"WARN","msg":"mesaj respins","component":"broker","event":"message.rejected","result":"rejected","correlationId":"2298d86f-4278-426d-b335-d7150396d006","error":"schemaVersion nesuportat: \"2.0\" (suportat: 1.0)"}
```

## Starea finală
`receiver-2` a primit „mesaj 2” și „mesaj 3”, iar după ele mesajele noi, live. Coada lui e goală.

## Garanția obținută
Cât timp **brokerul rămâne pornit**, un receiver care se reconectează cu același id primește
toate mesajele publicate pe topicurile lui cât a fost offline, în ordinea publicării,
până la 1000 de mesaje per receiver.

## Ce nu acoperă
- **Repornirea brokerului:** cozile sunt doar în memorie și se pierd.
- **Mesajele „în zbor”:** un mesaj pentru care `stream.Send` a reușit, dar pe care receiverul nu l-a
  citit înainte să cadă, se pierde. Receiverul nu confirmă mesajele.
- **Receiverul nou:** primește doar mesajele publicate după primul lui `Subscribe`.
- **Duplicatele:** dacă senderul primește `DEADLINE_EXCEEDED` și retrimite manual, mesajul poate
  ajunge de două ori, pentru că brokerul nu face deduplicare după `messageId`.
- **Doi receiveri cu același id:** ultimul conectat preia coada, iar celălalt primește `CANCELED` și se oprește.
