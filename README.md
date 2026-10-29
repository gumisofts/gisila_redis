# gisila_redis

Redis-backed stores for the [gisila](https://pub.dev/packages/gisila) web
framework, so rate limits and cached values hold across every isolate and
server that shares one Redis.

```dart
import 'dart:io';

import 'package:gisila/gisila.dart';
import 'package:gisila_redis/gisila_redis.dart';

final redis = GisilaRedis.fromUrl(Platform.environment['REDIS_URL']!);

final app = GisilaApp(
  config: AppConfig(
    rateLimit: const RateLimitConfig(requestsPerMinute: 300),
    rateLimitStore: RedisRateLimitStore(redis),
    cache: RedisCacheProvider(redis), // connects at start(), closes at stop()
  ),
);
```

## `RedisRateLimitStore`

Without it, each isolate keeps its own token buckets, so a server running
N isolates allows N times the configured rate. With it, `AppConfig.rateLimit`
and every per-route `RateLimitConfig` share one bucket per client across all
of them.

* Each request is one atomic Lua script (`EVALSHA`, reloaded after a
  `SCRIPT FLUSH`) that reads Redis's clock, so servers whose clocks drift
  still share one budget.
* Buckets expire once they'd be full again; idle clients cost nothing.
* When Redis is down, `failOpen: true` (the default) lets requests
  through so an outage doesn't take the API down. Pass `failOpen: false`
  for limits that guard against abuse, such as login attempts; those
  requests then get `429`. `onError` is called on every failure.

## `RedisCacheProvider`

An `AppConfig.cache` (`ctx.cache` in handlers) shared by every isolate:

* All keys live under `prefix` (`gisila:cache:` by default), so apps can
  share a Redis.
* `keys()` and `flush()` walk only that prefix with `SCAN`, never `KEYS`
  or `FLUSHDB`, so they don't block Redis or delete other apps' data.
* `increment` / `decrement` are atomic (`INCRBY`); `mget` and `mdelete`
  are single round trips.

## `GisilaRedis`

The client the stores use: one pipelined connection, opened lazily and
replaced after a timeout or socket error, so a Redis restart heals on
its own.

* `GisilaRedis.fromUrl('redis://[user[:password]@]host[:port][/db]')`, or
  `rediss://` for TLS (pass `securityContext:` for a private CA).
* `commandTimeout` (2 s) bounds every command: a hung Redis can't hang
  your requests. `connectTimeout` (5 s) bounds connecting.
* `send(['GET', 'key'])` for anything else; failures are
  `RedisException`.

## Testing

The tests need Redis on `localhost:6390` and are skipped without it:

```sh
docker run -d --rm --name gisila-redis-test -p 6390:6379 redis:7-alpine
dart test
docker stop gisila-redis-test
```
