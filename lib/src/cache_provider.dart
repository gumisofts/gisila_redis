/// A gisila [CacheProvider] in Redis.
library gisila_redis.cache_provider;

import 'package:gisila/gisila.dart' show CacheProvider;

import 'client.dart';

/// `AppConfig.cache` backed by Redis, shared by every isolate and server.
///
/// ```dart
/// AppConfig(cache: RedisCacheProvider(GisilaRedis.fromUrl(redisUrl)))
/// ```
///
/// [connect] opens the client at `start()`; [disconnect] closes it at
/// `stop()`, after the server has stopped taking requests, so the client
/// can be shared with a [RedisRateLimitStore].
///
/// Every key is stored under [prefix], so several apps can share one
/// Redis. [keys] and [flush] only see this prefix and walk it with `SCAN`
/// (never `KEYS` or `FLUSHDB`), so they don't block Redis or touch other
/// apps' data.
class RedisCacheProvider extends CacheProvider {
  RedisCacheProvider(this.redis, {this.prefix = 'gisila:cache:'});

  final GisilaRedis redis;
  final String prefix;

  String _k(String key) => '$prefix$key';

  @override
  Future<void> connect() => redis.connect();

  @override
  Future<void> disconnect() => redis.close();

  @override
  Future<String?> get(String key) async =>
      await redis.send(['GET', _k(key)]) as String?;

  @override
  Future<void> set(String key, String value, {Duration? ttl}) async {
    await redis.send([
      'SET',
      _k(key),
      value,
      if (ttl != null) ...['PX', '${ttl.inMilliseconds.clamp(1, 1 << 62)}'],
    ]);
  }

  @override
  Future<bool> delete(String key) async =>
      (await redis.send(['DEL', _k(key)]) as int) > 0;

  @override
  Future<bool> exists(String key) async =>
      (await redis.send(['EXISTS', _k(key)]) as int) > 0;

  @override
  Future<List<String>> keys(String pattern) async => [
        for (final k in await _scan(_escape(prefix) + pattern))
          k.substring(prefix.length)
      ];

  @override
  Future<void> flush() async {
    final all = await _scan('${_escape(prefix)}*');
    for (var i = 0; i < all.length; i += 500) {
      await redis.send(['DEL', ...all.skip(i).take(500)]);
    }
  }

  @override
  Future<int> increment(String key, {int by = 1}) async {
    try {
      return await redis.send(['INCRBY', _k(key), '$by']) as int;
    } on RedisException catch (e) {
      if (e.message.contains('not an integer')) {
        throw FormatException('Value at "$key" is not an integer');
      }
      rethrow;
    }
  }

  @override
  Future<int> decrement(String key, {int by = 1}) =>
      increment(key, by: -by);

  @override
  Future<Map<String, String>> mget(List<String> keys) async {
    if (keys.isEmpty) return {};
    final values = await redis.send(['MGET', ...keys.map(_k)]) as List;
    return {
      for (var i = 0; i < keys.length; i++)
        if (values[i] != null) keys[i]: values[i] as String,
    };
  }

  @override
  Future<int> mdelete(List<String> keys) async {
    if (keys.isEmpty) return 0;
    return await redis.send(['DEL', ...keys.map(_k)]) as int;
  }

  Future<List<String>> _scan(String match) async {
    final found = <String>{};
    var cursor = '0';
    do {
      final reply = await redis
          .send(['SCAN', cursor, 'MATCH', match, 'COUNT', '500']) as List;
      cursor = reply[0] as String;
      found.addAll((reply[1] as List).cast<String>());
    } while (cursor != '0');
    return found.toList();
  }

  /// Escapes glob characters in the prefix so only the caller's pattern
  /// is a pattern.
  static String _escape(String s) =>
      s.replaceAllMapped(RegExp(r'[*?\[\]\\]'), (m) => '\\${m[0]}');
}
