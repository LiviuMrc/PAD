# ADR 001: gRPC cu server streaming în locul socket-urilor TCP

**Status:** acceptat, lab2-v1.0

## Context
Partea 2 a lab-ului cere același broker (sender, broker, 2 receivere, rutare pe topic,
coadă per receiver, validare, procesare concurentă), dar implementat cu RPC în loc de
socket-uri și cu o reprezentare a datelor de tip proto/avro/thrift/graphql. Brokerul trebuie
să poată trimite mesaje receiverului din proprie inițiativă (push), nu doar să răspundă la cereri.

## Decizie
- **gRPC + Protocol Buffers.** Contractul e `contracts/broker.proto`, în pachetul `broker.v1`.
- **`Publish` e un RPC unary.** Răspunsul `PublishReply` ține locul ACK-ului, iar erorile sunt coduri
  de status gRPC (`INVALID_ARGUMENT`, `DEADLINE_EXCEEDED`, `UNAVAILABLE`).
- **`Subscribe` e un RPC cu server streaming.** Receiverul deschide un stream, iar brokerul îi trimite
  întâi coada, apoi mesajele noi. Header-ul stream-ului confirmă abonarea.
- **Validarea pe valori** (format ID, UTC, versiune, lungimi) rămâne în cod, în `common.Validate`,
  pentru că protobuf verifică doar tipurile.
- **Logica brokerului e aceeași ca în Lab 1:** `map[topic] → receivere` și `map[receiverId] → coadă`, protejate de un mutex.

## Alternative
- **Apache Thrift:** echivalent ca idee, dar are suport mai slab pentru streaming și ecosistem Go mai mic.
- **Receiverul face polling** (`rpc Poll` apelat periodic): simplu, dar adaugă latență și trafic inutil,
  dezavantaj menționat chiar în enunț. Streaming-ul livrează imediat.
- **Bidirectional streaming cu ACK de la receiver:** ar permite at-least-once, dar adaugă redelivery
  și deduplicare. Am ales să păstrez nivelul de complexitate al Lab 1.
- **Receiverul ca server gRPC** (brokerul îl apelează): brokerul ar trebui să știe adresa fiecărui
  receiver, iar receiverele din spatele unui NAT n-ar mai putea fi apelate.

## Consecințe
- Framing-ul, serializarea, concurența și deadline-urile sunt gestionate de gRPC. Codul de rețea
  dispare și rămâne doar logica brokerului.
- Contractul e tipizat și versionat. Testele de contract verifică numerele câmpurilor
  și toleranța la câmpuri noi.
- Proiectul are dependențe externe (`grpc`, `protobuf`) și un pas de generare de cod (`scripts/gen.sh`).
- Mesajele nu mai sunt lizibile în rețea (format binar). Pentru inspecție, `sender -json` afișează
  același mesaj în JSON.

## Limita cunoscută
> **Actualizare lab2-v1.2:** limitele de livrare și de persistență de mai jos au fost rezolvate prin
> `Ack`, jurnal pe disc și deduplicare. Vezi [ADR 002](002-livrare-at-least-once-si-persistenta.md).
> Brokerul a rămas un singur punct de eșec.

Ca în Lab 1, livrarea e **best-effort**. Un mesaj trimis cu `stream.Send` e considerat livrat, deși
receiverul poate cădea înainte să-l citească din buffer-ul HTTP/2. Cozile sunt în memorie și se pierd la
repornirea brokerului. Mai există și o limită nouă, specifică RPC: brokerul e un singur punct de eșec.
Dacă el cade, `Publish` întoarce `UNAVAILABLE`, iar senderul nu are unde să pună mesajul.
