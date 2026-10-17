# Lab 1: Message Broker (TCP + UDP multicast, XML/XSD)

Broker publish/subscribe minimal, scris în Go, numai cu biblioteca standard.
Are trei procese separate: **broker**, **sender**, **receiver**. Comunică doar prin
socket-uri: TCP, iar UDP pentru multicast. Nu folosește RPC și nici biblioteci de broker.

| Cerință | Unde |
|---|---|
| 3 componente separate | `broker/`, `sender/`, `receiver/` |
| TCP + UDP multicast | `common/transport.go` |
| Transport abstract (Send/Receive) | interfața `Transport`, implementată de `TCPTransport` și `UDPTransport` |
| Protocol: SUBSCRIBE, PUBLISH, ACK, ERROR | `common/message.go` |
| Rutare pe topic | `Broker.subs`: `map[topic] → set de receiverId` |
| Coadă în memorie per receiver | `Broker.queues`: `map[receiverId] → []Message` |
| Multicast UDP | topicul `broadcast` → grupul `239.0.0.1:9999` |
| XML parsat SAX | `common.ParseXML` (`xml.Decoder.Token()`) |
| Validare XSD | `contracts/message.xsd`. Brokerul răspunde `ERROR` la mesaje invalide |
| JSON | `Message.ToJSON()`, `sender -json`, `contracts/examples/valid-publish.json` |

Documentație: [arhitectură + diagramă](docs/architecture.md) ·
[ADR 001](docs/adr/001-broker-xml-peste-tcp.md) ·
[scenariul de eșec](docs/failure-scenario.md) · log-uri reale în [`docs/evidence/`](docs/evidence/)

## Cerințe

- Go 1.22+
- opțional: `xmllint` (pachetul `libxml2-utils`), folosit în testele de contract pentru validarea XSD reală
- pentru multicast, firewall-ul trebuie să permită UDP pe portul grupului. Cu ufw: `sudo ufw allow 9999/udp`

## Configurare

Variabilele sunt documentate în [`.env.example`](.env.example). Toate au valori implicite,
deci nu e nevoie să setezi nimic. Fiecare variabilă se poate suprascrie și prin flag (`-addr`, `-health`, `-multicast`, ...).

## Rulare manuală (demo cu 4 terminale)

```bash
# terminal 1: brokerul
go run ./broker

# terminal 2 și 3: două receivere
go run ./receiver -id receiver-1 -topics news -multicast
go run ./receiver -id receiver-2 -topics news,alerts -multicast

# terminal 4: senderul
go run ./sender -topic news -payload "salut"        # ajunge la ambele receivere
go run ./sender -topic alerts -payload "alertă"     # ajunge doar la receiver-2
go run ./sender -topic broadcast -payload "toți"    # ajunge la ambele, prin UDP multicast
go run ./sender -topic news                         # mod interactiv: o linie = un mesaj
```

**Receiver offline:** oprește receiver-2 (Ctrl+C), publică pe `news`, apoi pornește-l din nou
cu aceeași comandă. Primește imediat mesajele publicate cât a fost oprit.

**Validare:** `go run ./sender -raw contracts/examples/invalid-type.xml`. Brokerul răspunde
`ERROR: messageType necunoscut: "DELETE"`. Același lucru se întâmplă cu celelalte fișiere `invalid-*.xml`.

**JSON:** `go run ./sender -topic news -payload x -json` afișează același mesaj în XML și în JSON.

**Health check:**
```bash
curl localhost:7001/health/live    # {"status":"up"}: procesul rulează
curl localhost:7001/health/ready   # "ready", sau "degraded" dacă multicastul nu e disponibil
```

## Rulare automată

```bash
./scripts/demo.sh      # tot scenariul de mai sus. Log-urile ajung în docs/evidence/
```

## Teste

```bash
go test ./...                      # toate testele
go test -race -v ./...             # cu detalii și race detector
MULTICAST_REQUIRED=1 go test ./... # testul multicast eșuează în loc să fie sărit dacă grupul nu e accesibil
```

| Tip | Test |
|---|---|
| logică locală | `TestXMLRoundTrip`, `TestJSONIsSameMessage`, `TestPayloadWithNewlineStaysOnOneLine`, `TestParseRejectsBadStructure` |
| contract | `TestContractExamples` (codul și `xmllint --schema` dau același verdict pe fiecare exemplu), `TestGeneratedMessagesMatchXSD` |
| integrare | `TestPublishReachesBothReceivers`, `TestRoutingByTopic`, `TestHealth`, `TestMulticastBroadcast` |
| eșec | `TestInvalidMessagesAreRejected`, `TestBrokerOnlyAcceptsSubscribeAndPublish` |
| recuperare | `TestOfflineReceiverGetsQueuedMessagesOnReconnect` |

`TestMulticastBroadcast` este sărit (SKIP, cu mesaj explicativ) dacă firewall-ul blochează
datagramele multicast. Vezi secțiunea „Cerințe”.

## Protocol

Un mesaj XML pe linie, pe TCP:

```xml
<message>
  <messageId>3f2b8c1e-...</messageId>          <!-- unic per mesaj -->
  <correlationId>c0ffee00-...</correlationId>  <!-- același pe tot fluxul: sender → broker → receiver, plus ACK -->
  <messageType>PUBLISH</messageType>           <!-- SUBSCRIBE | PUBLISH | ACK | ERROR -->
  <schemaVersion>1.0</schemaVersion>
  <occurredAt>2026-10-17T10:00:00Z</occurredAt> <!-- UTC obligatoriu -->
  <sender>sender-1</sender>                     <!-- la SUBSCRIBE: id-ul receiverului -->
  <topic>news</topic>
  <payload>Salut</payload>                      <!-- opțional; la ACK = messageId confirmat, la ERROR = motivul -->
</message>
```

| Mesaj | Direcție | Răspuns |
|---|---|---|
| `SUBSCRIBE` | receiver → broker | `ACK`, urmat de mesajele din coadă |
| `PUBLISH` | sender → broker → receivere | `ACK` cu același `correlationId` |
| `ACK` | broker → client | n/a |
| `ERROR` | broker → client | conexiunea rămâne deschisă |

Semantica erorilor: XML malformat, element lipsă sau în ordine greșită, `messageType`
necunoscut, `schemaVersion` ≠ `1.0`, dată non-UTC, topic invalid sau payload > 4096 caractere
produc toate `ERROR` cu motivul în `payload`. Mesajul nu se rutează.
Un `schemaVersion` nou (de ex. 2.0) înseamnă un contract nou, deci un fișier XSD nou.

## DOM vs SAX

- **SAX** (folosit aici): parserul emite evenimente (`StartElement`, `CharData`, `EndElement`)
  pe măsură ce citește documentul, iar codul își păstrează singur starea. Memoria e constantă
  și se poate opri la prima eroare. În `ParseXML`, același parcurs verifică și ordinea
  elementelor cerută de `xs:sequence`. În Go: `xml.Decoder.Token()`.
- **DOM**: tot documentul se încarcă într-un arbore de noduri în memorie, care apoi poate fi
  parcurs și modificat liber (ex. `getElementsByTagName("topic")`). E mai comod pentru documente
  mari și navigare aleatoare, dar consumă memorie proporțională cu documentul. Go nu are un DOM
  în biblioteca standard. Cel mai apropiat e `xml.Unmarshal`, care încarcă tot documentul în struct-uri.
- Pentru mesaje mici și plate, cum sunt cele de aici, cele două sunt echivalente ca performanță.
  SAX a fost ales pentru că face parsarea și validarea structurii într-o singură trecere.

## Garanții și limite

**Garanții:**
- Fiecare mesaj are `messageId`, `correlationId`, `schemaVersion` și `occurredAt` în UTC.
  `correlationId` apare în log-urile sender, broker și receiver.
- Mesajele care nu respectă XSD-ul sunt respinse cu `ERROR` și nu ajung la receivere.
- Un mesaj publicat pe un topic ajunge la toți abonații online.
- Un receiver offline primește la reconectare, în ordine, mesajele din coadă, cât timp
  brokerul nu a fost repornit. Coada are maximum 1000 de mesaje per receiver.
- Fără retry infinit: receiverul face maximum `RECONNECT_MAX` reconectări. Senderul așteaptă
  ACK maximum 3s, iar scrierile TCP au deadline de 2s.

**Limite (intenționate pentru Lab 1):**
- Nu există persistență: la repornirea brokerului se pierd cozile.
- Receiverul nu confirmă mesajele. Un mesaj aflat „în zbor” când receiverul cade se poate pierde
  (best-effort, deci nici at-least-once, nici exactly-once).
- Nu există deduplicare, autentificare, replicare sau grupuri de consumatori.
- Multicastul UDP nu oferă garanții de livrare și nu folosește cozile.

Detalii: [docs/failure-scenario.md](docs/failure-scenario.md).

## Versiune

Versiunea evaluată are tag-ul `lab1-v1.0`. Corecturile ulterioare primesc tag-uri noi (`lab1-v1.1`, ...).
