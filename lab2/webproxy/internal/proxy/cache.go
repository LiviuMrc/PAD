package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"lab2/webproxy/internal/model"
)

// Cache-ul proxy-ului e în Redis (comun tuturor instanțelor proxy, persistent):
//
//	proxy:cache:<hash>  răspunsul (JSON), cu TTL = min(max-age de la DW, CacheTTL)
//	proxy:tag:<tag>     setul cheilor de cache care depind de tag (Cache-Tag de la DW)
//	proxy:gen           crește la fiecare invalidare; un răspuns obținut înainte de o
//	                    invalidare nu mai e salvat (altfel ar reintroduce date vechi)
//	proxy:stats         contoare hits/misses/bypass/stores/invalidations
type cache struct {
	rdb    *redis.Client
	maxTTL time.Duration
}

type cachedResponse struct {
	Status   int         `json:"status"`
	Header   http.Header `json:"header"`
	Body     []byte      `json:"body"`
	StoredAt time.Time   `json:"storedAt"`
}

const (
	keyGen   = "proxy:gen"
	keyStats = "proxy:stats"
	maxBody  = 1 << 20
)

// cacheKey depinde de metodă, cale, parametri (ordonați) și formatul negociat (XML/JSON):
// aceeași resursă în XML și în JSON sunt intrări diferite (Vary: Accept).
func cacheKey(r *http.Request, f model.Format) string {
	q := r.URL.Query()
	q.Del("format")
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(r.Method + " " + r.URL.Path + "?")
	for _, k := range keys {
		vs := q[k]
		sort.Strings(vs)
		for _, v := range vs {
			b.WriteString(url.QueryEscape(k) + "=" + url.QueryEscape(v) + "&")
		}
	}
	b.WriteString("|" + string(f))
	sum := sha256.Sum256([]byte(b.String()))
	return "proxy:cache:" + hex.EncodeToString(sum[:16])
}

func (c *cache) get(ctx context.Context, key string) (*cachedResponse, error) {
	raw, err := c.rdb.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var cr cachedResponse
	if err := json.Unmarshal(raw, &cr); err != nil {
		return nil, err
	}
	return &cr, nil
}

func (c *cache) generation(ctx context.Context) (int64, error) {
	n, err := c.rdb.Get(ctx, keyGen).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return n, err
}

// storeScript salvează răspunsul și îl leagă de tag-uri doar dacă proxy:gen nu s-a schimbat
// de la începutul cererii (atomic, în Redis).
var storeScript = redis.NewScript(`
local gen = tonumber(redis.call('GET', KEYS[1]) or '0')
if gen ~= tonumber(ARGV[1]) then return 0 end
redis.call('SET', KEYS[2], ARGV[2], 'PX', ARGV[3])
for i = 3, #KEYS do
  redis.call('SADD', KEYS[i], KEYS[2])
  local ttl = redis.call('PTTL', KEYS[i])
  if ttl < tonumber(ARGV[3]) then redis.call('PEXPIRE', KEYS[i], ARGV[3]) end
end
return 1`)

func (c *cache) store(ctx context.Context, key string, gen int64, cr *cachedResponse, ttl time.Duration, tags []string) (bool, error) {
	raw, err := json.Marshal(cr)
	if err != nil {
		return false, err
	}
	keys := []string{keyGen, key}
	for _, t := range tags {
		keys = append(keys, "proxy:tag:"+t)
	}
	n, err := storeScript.Run(ctx, c.rdb, keys, gen, raw, ttl.Milliseconds()).Int()
	return n == 1, err
}

// invalidateScript crește generația și șterge toate intrările legate de tag-uri.
var invalidateScript = redis.NewScript(`
redis.call('INCR', KEYS[1])
local n = 0
for i = 2, #KEYS do
  local members = redis.call('SMEMBERS', KEYS[i])
  for _, k in ipairs(members) do n = n + redis.call('DEL', k) end
  redis.call('DEL', KEYS[i])
end
return n`)

func (c *cache) invalidate(ctx context.Context, tags []string) (int, error) {
	keys := []string{keyGen}
	for _, t := range tags {
		keys = append(keys, "proxy:tag:"+t)
	}
	return invalidateScript.Run(ctx, c.rdb, keys).Int()
}

func (c *cache) count(ctx context.Context, field string) {
	c.rdb.HIncrBy(ctx, keyStats, field, 1)
}

// ttlFor întoarce cât poate sta răspunsul în cache (0 = nu se cache-uiește), după
// Cache-Control-ul de la DW: doar public + max-age > 0, fără no-store/private/no-cache.
func (c *cache) ttlFor(h http.Header) time.Duration {
	var maxAge time.Duration
	public := false
	for _, d := range strings.Split(h.Get("Cache-Control"), ",") {
		d = strings.ToLower(strings.TrimSpace(d))
		switch {
		case d == "no-store", d == "private", d == "no-cache":
			return 0
		case d == "public":
			public = true
		case strings.HasPrefix(d, "max-age="):
			if n, err := strconv.Atoi(strings.TrimPrefix(d, "max-age=")); err == nil && n > 0 {
				maxAge = time.Duration(n) * time.Second
			}
		}
	}
	if !public || maxAge == 0 {
		return 0
	}
	return min(maxAge, c.maxTTL)
}

func parseTags(h string) []string {
	var out []string
	for _, t := range strings.Split(h, ",") {
		if t = strings.TrimSpace(t); t != "" {
			out = append(out, t)
		}
	}
	return out
}

// requestBypass: clientul cere explicit un răspuns proaspăt (no-cache) sau interzice
// salvarea (no-store).
func requestBypass(r *http.Request) (skipLookup, skipStore bool) {
	for _, d := range strings.Split(r.Header.Get("Cache-Control"), ",") {
		switch strings.ToLower(strings.TrimSpace(d)) {
		case "no-cache", "max-age=0":
			skipLookup = true
		case "no-store":
			skipLookup, skipStore = true, true
		}
	}
	if r.Header.Get("Pragma") == "no-cache" {
		skipLookup = true
	}
	if r.Header.Get("Authorization") != "" {
		skipLookup, skipStore = true, true
	}
	return
}
