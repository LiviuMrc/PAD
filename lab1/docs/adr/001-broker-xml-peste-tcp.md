# ADR 001: Broker propriu, cu mesaje XML pe TCP și cozi în memorie per receiver

**Status:** acceptat, lab1-v1.0

## Context
Lab 1 cere un broker publish/subscribe cu sender și receivere separate. Comunicarea
se face doar prin socket-uri (TCP, iar UDP pentru multicast), fără RPC și fără biblioteci
de broker. Mesajele sunt în XML, validate cu o schemă XSD. Un receiver offline trebuie
să primească mesajele la reconectare. O versiune anterioară a proiectului avea gRPC, DLQ,
retry cu backoff și deduplicare. Era greu de explicat și nu respecta „fără RPC”.

## Decizie
- **Transport:** socket TCP direct, cu un mesaj XML pe linie (`\n` ca delimitator), în spatele
  unei interfețe `Transport { Send; Receive; Close }`. UDP multicast doar pentru topicul `broadcast`.
- **Protocol:** 4 tipuri: `SUBSCRIBE`, `PUBLISH`, `ACK`, `ERROR`. Fiecare mesaj are
  `messageId`, `correlationId`, `messageType`, `schemaVersion`, `occurredAt` (UTC), `sender`, `topic`, `payload`.
- **Parsare:** SAX/stream (`xml.Decoder.Token()`). Mesajele sunt mici, iar validarea
  structurii se face pe evenimente, în același pas.
- **Validare:** Go nu are validator XSD în biblioteca standard. Regulile din `message.xsd`
  sunt implementate în `Validate()`, iar testul de contract rulează `xmllint --schema`
  pe aceleași exemple și verifică că rezultatul coincide. Fără dependențe externe la runtime.
- **Stocare:** `map[receiverId][]Message` în memorie. Un mesaj e scos din coadă după ce
  scrierea în socket reușește.

## Alternative
- **gRPC / RPC:** interzis de cerințe și ascunde exact comunicarea care trebuie demonstrată.
- **RabbitMQ/Kafka/NATS:** interzise. Nu s-ar mai vedea rutarea și cozile.
- **libxml2 prin cgo pentru XSD:** validare „reală” la runtime, dar cu cgo și dependență
  nativă. Ar complica build-ul pentru un câștig mic.
- **DOM (încărcarea întregului arbore):** corect, dar fără avantaj la mesaje plate de 8 câmpuri.
- **Length-prefix în loc de `\n`:** robust pentru date binare, dar nu se poate testa ușor cu `nc`.
- **ACK de la receiver + redelivery:** ar da at-least-once, dar adaugă duplicate,
  deduplicare și timeouts. Tăiat intenționat.

## Consecințe
- Codul are în jur de 900 de linii fără teste, numai cu biblioteca standard Go, și se explică ușor la demo.
- Un receiver identificat prin același `receiverId` primește la reconectare mesajele din coadă, în ordine.
- XSD-ul e sursa contractului, dar la runtime regulile sunt duplicate în cod. Testul de contract prinde divergențele.

## Limita cunoscută
Livrarea e **best-effort, fără confirmare de la receiver**. Dacă receiverul moare după ce
mesajul a ajuns în buffer-ul TCP, dar înainte să-l citească, mesajul se pierde. La repornirea
brokerului se pierd toate cozile (nu există persistență pe disc). Multicastul UDP nu are nicio
garanție de livrare și nu ajunge la receiverele offline.
