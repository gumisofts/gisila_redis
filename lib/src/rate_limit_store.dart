/// A [RateLimitStore] in Redis, shared by every isolate and server.
library gisila_redis.rate_limit_store;

import 'dart:async';

import 'package:gisila/gisila.dart' show RateLimitStore;

import 'client.dart';

/// Token buckets in Redis, so `AppConfig.rateLimit` and per-route limits
/// hold across every isolate and server that uses the same Redis.
///
/// ```dart
/// final redis = GisilaRedis.fromUrl(env['REDIS_URL']!);
/// AppConfig(rateLimitStore: RedisRateLimitStore(redis), ...)
/// ```
///
/// Each take is one atomic Lua script that uses Redis's clock, so servers
/// with drifting clocks share one budget. Buckets expire once they would
/// be full again.
///
/// When Redis fails, [failOpen] (the default) lets the request through
/// so an outage doesn't take the API down; pass `false` to answer 429
/// instead, for limits that guard against abuse such as login attempts.
/// [onError] hears about every failure either way.
class RedisRateLimitStore implements RateLimitStore {
  RedisRateLimitStore(
    this.redis, {
    this.prefix = 'gisila:rl:',
    this.failOpen = true,
    this.onError,
    this.failClosedRetry = const Duration(seconds: 1),
  });

  final GisilaRedis redis;

  /// Prepended to every bucket key.
  final String prefix;
  final bool failOpen;
  final void Function(Object error)? onError;

  /// The wait reported when failing closed.
  final Duration failClosedRetry;

  String? _sha;

  // KEYS[1] bucket; ARGV[1] capacity, ARGV[2] refill per second.
  // Returns the seconds to wait as a string (Lua numbers would be
  // truncated to integers in the reply), "0" when a token was taken.
  static const _script = '''
local cap = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local b = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(b[1])
local ts = tonumber(b[2])
if tokens == nil or ts == nil then
  tokens = cap
  ts = now
end
tokens = math.min(cap, tokens + math.max(0, now - ts) * rate)
local wait = 0
if tokens < 1 then
  wait = (1 - tokens) / rate
else
  tokens = tokens - 1
end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
redis.call('PEXPIRE', KEYS[1], math.ceil(cap / rate * 1000) + 1000)
return tostring(wait)
''';

  @override
  Future<Duration?> take(
    String key, {
    required double capacity,
    required double refillPerSecond,
  }) async {
    final Object? reply;
    try {
      reply = await _eval('$prefix$key', capacity, refillPerSecond);
    } catch (e) {
      onError?.call(e);
      return failOpen ? null : failClosedRetry;
    }
    final wait = double.tryParse('$reply') ?? 0;
    if (wait <= 0) return null;
    return Duration(milliseconds: (wait * 1000).ceil());
  }

  Future<Object?> _eval(String key, double capacity, double rate) async {
    final args = <Object>['1', key, '$capacity', '$rate'];
    final sha = _sha;
    if (sha != null) {
      try {
        return await redis.send(['EVALSHA', sha, ...args]);
      } on RedisException catch (e) {
        // The script cache was flushed (restart, SCRIPT FLUSH).
        if (!e.message.startsWith('NOSCRIPT')) rethrow;
      }
    }
    _sha = await redis.send(['SCRIPT', 'LOAD', _script]) as String;
    return redis.send(['EVALSHA', _sha!, ...args]);
  }
}
