# Changelog

## 0.1.0 — 2026-10-30

- `RedisRateLimitStore`: a gisila `RateLimitStore` shared by every isolate
  and server (atomic Lua token bucket on Redis's clock; fails open by
  default, `failOpen: false` for abuse limits).
- `RedisCacheProvider`: a gisila `CacheProvider` under a key prefix;
  `keys` and `flush` use `SCAN` and stay inside the prefix.
- `GisilaRedis`: lazily connecting, self-healing client with connect and
  command timeouts, `redis://` / `rediss://` URLs, AUTH and SELECT.
- Works with gisila 0.3 and 0.4.
