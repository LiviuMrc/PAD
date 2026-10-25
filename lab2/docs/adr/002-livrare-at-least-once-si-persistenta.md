# ADR 002: livrare at-least-once, jurnal pe disc și deduplicare

**Status:** acceptat, lab2-v1.2. Înlocuiește secțiunea „Limita cunoscută” din [ADR 001](001-grpc-in-loc-de-socketuri.md).

## Context
În v1.1 livrarea era best-effort: brokerul scotea mesajul din coadă imediat ce `stream.Send` reușea,
adică imediat ce mesajul ajungea în buffer-ul HTTP/2, nu la receiver. Un receiver care cădea înainte
să citească mesajul îl pierdea definitiv, la fel o sesiune preluată de alta cu același id. Cozile erau
doar în memorie, iar un sender care retrimitea după `DEADLINE_EXCEEDED` putea dubla mesajul.
Testele de reproducere au confirmat fiecare caz pe codul v1.1.

## Decizie
- **`rpc Ack` (unary), nu bidirectional streaming.** Un mesaj rămâne în coadă până la `Ack`. Fiecare
  receiver are un cursor `sent`: câte mesaje din capul cozii a trimis sesiunea curentă. O sesiune nouă
  pornește cursorul de la 0, deci retrimite tot ce nu e confirmat. Doar sesiunea curentă avansează
  cursorul, așa că o sesiune înlocuită nu mai poate „lua” un mesaj. Un RPC unary separat păstrează
  `Subscribe` neschimbat pentru clienții existenți (grpcurl, clienți vechi).
- **Jurnal append-only (JSON lines) cu fsync.** Fiecare operație (`sub`, `pub`, `ack`, `unsub`) e
  scrisă și sincronizată pe disc *înainte* de răspuns, sub același mutex ca starea din memorie, deci
  ordinea din jurnal e ordinea reală. La pornire, înregistrările sunt aplicate din nou prin aceleași
  funcții `apply*`. O ultimă linie incompletă (cădere în timpul scrierii) e tăiată: brokerul nu a
  confirmat-o niciodată. Când jurnalul depășește de 2 ori starea curentă, e rescris ca snapshot
  (fișier temporar + `rename` + fsync pe director, deci atomic).
- **Deduplicare pe `message_id`** (ultimele 10000, salvate și în snapshot). Un `Publish` repetat
  primește `duplicate = true` și nu e pus din nou în cozi, deci senderul poate reîncerca sigur
  la `UNAVAILABLE` și `DEADLINE_EXCEEDED`.
- **Receiverul ignoră retrimiterile** deja procesate (după `message_id`), dar le confirmă.

## Alternative
- **Bidirectional streaming cu ACK pe același stream:** mai puține conexiuni, dar schimbă semnătura
  lui `Subscribe` (incompatibil cu v1) și complică ordinea închiderii.
- **Bază de date (SQLite, BoltDB):** mai robustă, dar adaugă dependențe pentru o stare de câteva mii
  de mesaje. Jurnalul are sub 300 de linii de cod și e ușor de citit și de testat.
- **Snapshot complet la fiecare operație:** simplu, dar O(stare) per mesaj.

## Consecințe
- Garanția devine **at-least-once**: nimic confirmat senderului nu se pierde, nici la `kill -9`, dar un
  mesaj poate ajunge de două ori la un receiver repornit (deduplicarea lui e în memorie).
- Fiecare `Publish` și `Ack` costă un fsync (~1 ms pe SSD). `BROKER_FSYNC=false` îl dezactivează,
  cu riscul de a pierde ultimele operații la o cădere a sistemului (nu doar a procesului).
- Receiverele care nu se mai întorc ar ține cozile la nesfârșit, așa că există `Unsubscribe` și un
  TTL (`RECEIVER_TTL_SEC`, implicit 10 minute) după care un receiver offline e uitat.
