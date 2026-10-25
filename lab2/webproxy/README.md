# Lab 2: Web proxy — realizarea transparenței în distribuire

Implementarea lucrării din [`../cerinte.pdf`](../cerinte.pdf): un nod informativ de tip data
warehouse (DW) accesat prin HTTP și un reverse proxy cu smart-proxy, caching și load balancing.
Tot sistemul rulează cu componente reale: **Cassandra 4.1** (datele DW) și **Redis 7** (cache-ul
și evidența conexiunilor proxy-ului), pornite cu Docker. Nu există implementări simulate,
nici în cod, nici în teste.

```
                         ┌──────────────── proxy (:9100) ─────────────────┐
 clienți (curl, ...) ──► │ smart proxy: keep-alive + evidența conexiunilor │──► Redis (:16379)
 noduri de stocare   ──► │ cache GET (TTL, ETag, invalidare Cache-Tag)     │    cache, conexiuni
 (src1 :9111, src2 :9112)│ load balancing: round-robin / least-connections │
                         │ health checks + failover                        │
                         └───────┬───────────────┬───────────────┬─────────┘
                                 ▼               ▼               ▼
                            dw1 (:9101)     dw2 (:9102)     dw3 (:9103)     Controllere HTTP,
                                 └───────────────┼───────────────┘          XML / JSON
                                                 ▼
                                       Cassandra (:19042)  — o singură instanță, LWT
                 push (POST /push) ◄── DW         pull (GET /update/employees/) ── surse
```

## Cerințele din PDF și unde sunt implementate

| Cerință | Implementare | Dovadă |
|---|---|---|
| **Etapa 1:** nod informativ DW, comunicare prin HTTP | `warehouse/`, `internal/dw/` | `internal/dw/dw_test.go` |
| GET pentru date: `GET /employee/?id=1`, `GET /employees/?offset=1&limit=4` | `employeeController.get`, `employeesController.list` | `TestListPagination`, demo |
| PUT de la nodurile de stocare | `employeeController.put`; nodul `source/` trimite datele la pornire | `TestSyncAllPutsDataThroughProxy`, demo |
| POST cu date de modificat + nodul sursă cere modificările (`GET /update/employees/`, pull) | `employeeController.post`, `updatesController`; `source.Pull` | `TestPullAppliesModificationsAndPersists`, demo |
| *Opțional:* PUSH — DW inițiază cererea spre nodul sursă | `internal/dw/push.go`, `POST /subscriptions/`; `source` primește pe `POST /push` | `TestPushNotifiesSubscribers`, `TestPushDeliversModifications`, demo |
| Procesare concurentă, thread-per-request | `net/http`: fiecare cerere în curs are propria goroutine; `/metrics` arată `maxInflight` | `TestConcurrentRequests`, demo (`maxInflight` ≥ 2) |
| Acces simultan: colecții thread-safe / excludere reciprocă | Cassandra LWT (`UPDATE ... IF version = ?`) între toate instanțele DW; în proces: atomice, mutex, canale | `TestConcurrentModifyLosesNothing` (4 sesiuni, 40 de modificări), demo (30 de clienți prin 3 DW) |
| Controllere | `employeeController`, `employeesController`, `updatesController`, `subscriptionController` (`internal/dw/controllers.go`) | — |
| Clientul alege formatul (XML / JSON) | `model.Negotiate`: `?format=xml\|json` sau `Accept` (cu q-values); corpul cererii după `Content-Type` | `TestFormatsXMLAndJSON`, `TestNegotiate`, demo |
| **Etapa 2:** toată comunicarea prin proxy | `proxy/`, `internal/proxy/`; sursele și clienții folosesc doar `:9100` | demo |
| Smart proxy, păstrarea conexiunilor spre clienți | keep-alive (`IdleTimeout` 120s), evidența fiecărei conexiuni în Redis (`internal/proxy/conns.go`), pool keep-alive spre DW | `TestConnectionRegistryInRedis`, demo, `GET /_proxy/connections` |
| Caching | Redis: TTL din `Cache-Control: max-age`, `Vary: Accept`, ETag/304, invalidare la scrieri prin `Cache-Tag`, fără date vechi după invalidare | `TestCacheHitMissAndInvalidation`, `TestCacheTTLExpires`, `TestCacheRespectsDirectives`, `TestStaleResponseIsNotStoredAfterInvalidation`, demo |
| Load balancing, mai multe noduri DW, algoritm selectat | `round-robin` (implicit) sau `least-connections` (`-lb`), health checks, failover | `TestRoundRobin`, `TestLeastConnections`, `TestFailoverAndRecovery`, demo |
| *Recomandare:* Cassandra, o singură instanță pentru toate DW-urile | `internal/store/` | toate testele DW/proxy/sursă |
| *Recomandare:* Redis pentru proxy, key-value persistent | `--appendonly yes`; cache și conexiuni în Redis, comune instanțelor proxy | `TestCacheSharedBetweenProxies`, demo (HIT după repornirea proxy-ului) |

## Rulare

Cerințe: Go 1.22+, Docker cu Compose, `curl`. Toate comenzile se rulează din `lab2/`.

```bash
./webproxy/scripts/validate.sh     # tot: infrastructură, vet, teste (-race, niciunul sărit), demo
```

Manual, în terminale separate:

```bash
./webproxy/scripts/infra-up.sh                                   # Cassandra + Redis (docker compose)

DW_ID=dw1 DW_ADDR=127.0.0.1:9101 go run ./webproxy/warehouse     # primul DW creează schema
DW_ID=dw2 DW_ADDR=127.0.0.1:9102 go run ./webproxy/warehouse
DW_ID=dw3 DW_ADDR=127.0.0.1:9103 go run ./webproxy/warehouse
go run ./webproxy/proxy                                          # :9100, round-robin
go run ./webproxy/proxy -lb least-connections                    # sau alt algoritm

go run ./webproxy/source -id src1 -seed webproxy/data/src1.json -format xml -addr 127.0.0.1:9111
go run ./webproxy/source -id src2 -seed webproxy/data/src2.json -addr 127.0.0.1:9112 -push=false
```

Exemple (clientul vorbește doar cu proxy-ul):

```bash
curl -i 'http://127.0.0.1:9100/employee/?id=1'                              # JSON, X-Cache: MISS
curl -i 'http://127.0.0.1:9100/employee/?id=1'                              # X-Cache: HIT
curl    'http://127.0.0.1:9100/employees/?offset=1&limit=4' -H 'Accept: application/xml'
curl -X POST http://127.0.0.1:9100/employee/ -H 'Content-Type: application/json' \
     -d '{"id":1,"salary":6000}'                                            # invalidează cache-ul
curl    'http://127.0.0.1:9100/update/employees/?since='                    # feed-ul pentru pull
curl    http://127.0.0.1:9111/employees/                                    # datele locale ale src1
curl    http://127.0.0.1:9100/_proxy/backends                               # DW-uri, sănătate, cereri
curl    http://127.0.0.1:9100/_proxy/connections                            # conexiunile clienților (Redis)
curl    http://127.0.0.1:9100/_proxy/stats                                  # hits/misses/invalidări
```

Oprire: Ctrl+C în fiecare terminal, apoi `docker compose -f webproxy/docker-compose.yml down`
(adaugă `-v` ca să ștergi și datele Cassandra/Redis).

## API-ul DW

| Metodă și cale | Rol | Răspuns |
|---|---|---|
| `GET /employee/?id=N` | un angajat | 200, 404; `ETag: "N-v<versiune>-<format>"`, `Cache-Control: public, max-age=30` |
| `GET /employees/?offset=O&limit=L` | o pagină (L ≤ 100, O ≤ 10000) | 200 `{offset, limit, total, employees}` |
| `PUT /employee/` | nodul de stocare creează/înlocuiește entitatea | 201 + `Location`, sau 200 |
| `POST /employee/` | modificare parțială (`{"id":1,"salary":6000}`); `If-Match` = ETag-ul citit | 200; 412 dacă versiunea s-a schimbat; 404 |
| `DELETE /employee/?id=N` | ștergere (`If-Match` opțional) | 204, 404, 412 |
| `GET /update/employees/?since=<seq>&overlap=5s&limit=` | modificările făcute prin POST/DELETE (pull) | 200 `{since, next, changes}`, `no-store` |
| `POST /subscriptions/` `{"url","source","format"}` | abonare push | 201 |
| `DELETE /subscriptions/?url=` | dezabonare | 204 |
| `GET /health`, `GET /metrics` | starea Cassandra; cereri, `inflight`, `maxInflight`, push trimise/eșuate | 200 / 503 |

Toate răspunsurile (inclusiv erorile `{status, message}`) vin în formatul cerut. Corpurile cererilor
pot fi XML sau JSON (`Content-Type`); alt tip → 415; `Accept` fără XML/JSON → 406; metodă greșită → 405
cu `Allow`. Fiecare răspuns are `X-Served-By` (instanța DW); prin proxy apar și `X-Cache`
(`HIT`/`MISS`/`BYPASS`), `X-Backend`, `X-Proxy`.

## Cum funcționează

**Concurență în DW.** `net/http` rulează fiecare conexiune într-o goroutine, iar cererile unei
conexiuni HTTP/1.1 sunt tratate pe rând, deci fiecare cerere în curs are propriul fir de execuție
(thread-per-request; goroutine-urile sunt planificate de Go pe firele sistemului). Datele comune
stau în Cassandra: fiecare scriere e o tranzacție LWT condiționată de versiunea citită, deci două
instanțe DW care modifică simultan același angajat nu își pot pierde una alteia modificarea
(la conflict, operația se reia sau clientul primește 412). În proces, contoarele sunt atomice,
lista de abonați push e protejată de un mutex, iar notificările trec printr-un canal.

**Pull și push.** `PUT` vine de la nodul care deține datele și nu intră în feed. `POST` și `DELETE`
(modificări făcute de clienți) sunt scrise în tabelul `employee_changes` (timeuuid, păstrat 7 zile).
Nodul sursă citește feed-ul cu un cursor și o fereastră de suprapunere (`overlap=5s`), ca să prindă
și modificările făcute concurent pe alte instanțe DW, și aplică fiecare modificare o singură dată
(după `seq`) și doar pentru entitățile lui. Push-ul e asincron, cu 3 reîncercări; dacă se pierde,
pull-ul o aduce oricum. Starea sursei (date + cursor) e salvată atomic pe disc.

**Cache.** Doar `GET` cu `Cache-Control: public, max-age>0` de la DW, TTL = min(max-age,
`CACHE_TTL_SEC`). Cheia include calea, parametrii (ordonați) și formatul negociat. DW marchează
răspunsurile cu `Cache-Tag` (`employee:N`, `employees`); o scriere reușită prin proxy șterge toate
intrările cu aceleași tag-uri (script Lua, atomic). Un contor de generație împiedică salvarea unui
răspuns obținut înainte de invalidare. `Cache-Control: no-cache` de la client ocolește cache-ul.

**Load balancing și failover.** Health check pe `GET /health` la fiecare 2 s. O eroare de conexiune
scoate backend-ul din rotație imediat, iar cererea e reluată pe alt DW: orice metodă dacă cererea
nu a plecat, doar metodele idempotente (GET, PUT, DELETE) dacă e posibil să fi ajuns la DW.

## Configurare

| Variabilă | Implicit | Componentă |
|---|---|---|
| `DW_ID`, `DW_ADDR` | `dw1`, `127.0.0.1:9101` | warehouse |
| `CASSANDRA_HOSTS`, `CASSANDRA_KEYSPACE` | `127.0.0.1:19042`, `lab2_dw` | warehouse |
| `CACHE_MAX_AGE_SEC` | `30` | warehouse (Cache-Control) |
| `PROXY_ID`, `PROXY_ADDR` | `proxy1`, `127.0.0.1:9100` | proxy |
| `DW_BACKENDS` | cele 3 DW de pe 9101-9103 | proxy |
| `LB_ALGORITHM` | `round-robin` (sau `least-connections`) | proxy |
| `REDIS_ADDR`, `CACHE_TTL_SEC`, `HEALTH_INTERVAL_MS` | `127.0.0.1:16379`, `60`, `2000` | proxy |
| `SOURCE_ID`, `SOURCE_ADDR`, `PROXY_URL`, `SOURCE_SEED`, `SOURCE_FORMAT`, `SOURCE_POLL_MS`, `SOURCE_PUSH` | `src1`, `127.0.0.1:9111`, proxy-ul, —, `json`, `2000`, `true` | source |

Porturile Cassandra/Redis (19042/16379) diferă de cele standard ca să nu intre în conflict cu
serviciile din `1lab`.

## Teste

```bash
./webproxy/scripts/infra-up.sh
go test -race ./webproxy/...
```

Testele folosesc Cassandra și Redis reale (fiecare pachet are keyspace-ul și baza Redis proprii).
Dacă infrastructura nu rulează, sunt sărite cu un mesaj; `validate.sh` eșuează dacă vreun test
e sărit. Rezultatul complet al ultimei rulări a demo-ului e în
[`docs/evidence/demo.txt`](docs/evidence/demo.txt), împreună cu log-urile fiecărui proces.

## Limite cunoscute

- Toți angajații stau într-o singură partiție Cassandra (ordonați după id), ceea ce face paginarea
  simplă, dar nu scalează la milioane de rânduri. Cassandra nu are `OFFSET`: se citesc `offset+limit`
  rânduri, deci `offset` e limitat la 10000.
- Scrierea în feed-ul de modificări vine după tranzacția LWT, nu în aceeași tranzacție (Cassandra
  nu permite LWT pe mai multe tabele). Dacă procesul DW cade exact între ele, modificarea e salvată,
  dar nodul sursă nu o mai primește prin pull.
- Cache-ul e invalidat de scrierile care trec prin proxy (toate, în această arhitectură); o scriere
  directă în DW ar fi vizibilă abia după expirarea TTL-ului.
- Fără autentificare și TLS (doar `127.0.0.1`).
