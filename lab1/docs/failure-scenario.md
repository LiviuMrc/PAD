# Scenariul de eșec: receiver offline, apoi reconectat

Reproducere automată: `go test ./broker -run Offline -v`
(test `TestOfflineReceiverGetsQueuedMessagesOnReconnect`)
Reproducere cu procese reale: `./scripts/demo.sh`. Log-urile se scriu în `docs/evidence/`.

## Starea inițială
Brokerul rulează. `receiver-1` e abonat la `news`. `receiver-2` e abonat la `news` și `alerts`.
Ambele primesc „mesaj 1”.

## Injectarea eșecului
Procesul `receiver-2` e oprit (`kill`). Brokerul observă închiderea conexiunii TCP:

```json
{"time":"2026-10-17T15:20:08.216692324Z","level":"INFO","msg":"receiver deconectat, mesajele noi rămân în coadă","component":"broker","event":"receiver.offline","receiver":"receiver-2"}
```

## Comportamentul observat
Senderul publică „mesaj 2” (news) și „mesaj 3” (alerts) și primește `ACK` pentru amândouă.
`receiver-1` primește „mesaj 2”. Mesajele pentru `receiver-2` rămân în coada lui.
Health check-ul arată un singur receiver online:

```json
{"multicast":true,"onlineReceivers":1,"status":"ready","topics":2}
```

## Mecanismul de recuperare
`receiver-2` repornește cu **același id** și trimite `SUBSCRIBE`. Brokerul vede 2 mesaje
în coadă (`queued:2`) și le livrează în ordine:

```json
{"time":"2026-10-17T15:20:08.535110777Z","level":"INFO","msg":"receiver abonat","component":"broker","event":"subscribe","result":"ok","correlationId":"ce7e6639-3bd8-43bb-bc39-e8aca19974ca","messageId":"ce021977-2dab-491e-8f9c-7542f5b3ef58","receiver":"receiver-2","topic":"news","queued":2}
{"time":"2026-10-17T15:20:08.535170669Z","level":"INFO","msg":"mesaj livrat","component":"broker","event":"deliver","result":"ok","receiver":"receiver-2","correlationId":"a1592f14-7d0c-424e-b44b-c2ac0dbe20f6","messageId":"f5480b71-3cbf-4fd3-a2ae-bd7a0ddb4791"}
{"time":"2026-10-17T15:20:08.535208532Z","level":"INFO","msg":"mesaj livrat","component":"broker","event":"deliver","result":"ok","receiver":"receiver-2","correlationId":"628c367e-0726-42cf-8050-f874e038857d","messageId":"2bdb07cb-ce0a-40a7-ad7b-161eb5e6535a"}
```

Dacă brokerul e cel care cade, receiverul se reconectează de cel mult `RECONNECT_MAX` ori
(implicit 5), la `RECONNECT_INTERVAL_SEC` secunde (implicit 2), apoi se oprește. Nu există retry infinit:

```json
{"msg":"conexiune pierdută, reîncerc","component":"receiver","event":"reconnect","attempt":2,"max":2,"in":"200ms"}
{"msg":"renunț la reconectare","component":"receiver","event":"reconnect","result":"gave_up","attempts":2}
```

## Starea finală
`receiver-2` a primit „mesaj 2” și „mesaj 3”, iar după ele mesajele noi, live.
Coada lui din broker e goală (verificat în test: `len(queues["receiver-2"]) == 0`).

## Garanția obținută
Cât timp **brokerul rămâne pornit**, un receiver care se reconectează cu același id
primește toate mesajele publicate pe topicurile lui cât a fost offline, în ordinea
publicării, până la 1000 de mesaje per receiver. Peste limită se elimină cel mai vechi,
cu log `queue.overflow`.

## Ce nu acoperă
- **Repornirea brokerului:** cozile sunt doar în memorie și se pierd.
- **Mesajele „în zbor”:** un mesaj scris cu succes în socket, dar necitit de un receiver care
  tocmai a murit, e considerat livrat și se pierde. Receiverul nu trimite ACK.
- **Receiverul nou:** primește doar mesajele publicate după primul lui `SUBSCRIBE`.
- **Multicastul UDP:** nu se pune în coadă. Un receiver offline nu primește mesajele de pe `broadcast`.
- **Duplicatele:** un sender care retrimite manual același mesaj produce două livrări,
  pentru că brokerul nu face deduplicare.
