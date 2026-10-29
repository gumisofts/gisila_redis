/// Needs Redis on `localhost:6390`, e.g.
/// `docker run -d --rm --name gisila-redis-test -p 6390:6379 redis:7-alpine`.
/// Skipped when it isn't reachable.
library;

import 'dart:io';

import 'package:gisila/gisila.dart';
import 'package:gisila_redis/gisila_redis.dart';
import 'package:test/test.dart';

const _port = 6390;

Future<bool> _redisUp() async {
  try {
    final s = await Socket.connect('localhost', _port,
        timeout: const Duration(seconds: 1));
    s.destroy();
    return true;
  } catch (_) {
    return false;
  }
}

Future<int> _unusedPort() async {
  final s = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
  final port = s.port;
  await s.close();
  return port;
}

void main() {
  group('GisilaRedis.fromUrl', () {
    test('parses credentials, database and TLS', () {
      final r = GisilaRedis.fromUrl('rediss://app:p%40ss@cache.internal:6380/2');
      expect([r.host, r.port, r.username, r.password, r.database, r.tls],
          ['cache.internal', 6380, 'app', 'p@ss', 2, true]);
      final bare = GisilaRedis.fromUrl('redis://:secret@localhost');
      expect([bare.port, bare.username, bare.password, bare.database, bare.tls],
          [6379, null, 'secret', 0, false]);
      expect(GisilaRedis.fromUrl('redis://onlypass@h').password, 'onlypass');
      expect(() => GisilaRedis.fromUrl('http://h'), throwsFormatException);
    });
  });

  late bool up;
  setUpAll(() async => up = await _redisUp());

  void redisTest(String name, Future<void> Function() body) {
    test(name, () async {
      if (!up) {
        markTestSkipped('No Redis on localhost:$_port');
        return;
      }
      await body();
    });
  }

  late GisilaRedis redis;
  setUp(() async {
    redis = GisilaRedis(port: _port);
    if (up) await redis.send(['FLUSHDB']);
  });
  tearDown(() => redis.close());

  group('GisilaRedis', () {
    redisTest('reconnects after the connection is killed', () async {
      expect(await redis.send(['PING']), 'PONG');
      final other = GisilaRedis(port: _port);
      await other.send([
        'CLIENT', 'KILL', 'TYPE', 'normal', 'SKIPME', 'yes', //
      ]);
      await other.close();
      try {
        await redis.send(['PING']);
      } on RedisException {
        // The first command after the kill may fail...
      }
      expect(await redis.send(['PING']), 'PONG'); // ...then it heals.
    });

    redisTest('a slow command times out and the client recovers', () async {
      final quick = GisilaRedis(
          port: _port, commandTimeout: const Duration(milliseconds: 200));
      addTearDown(quick.close);
      await expectLater(quick.send(['BLPOP', 'nothing-here', '1']),
          throwsA(isA<RedisException>()));
      expect(await quick.send(['PING']), 'PONG');
    });

    redisTest('a failed AUTH is reported and retried, not stuck', () async {
      final wrong = GisilaRedis(port: _port, password: 'nope');
      addTearDown(wrong.close);
      for (var i = 0; i < 2; i++) {
        await expectLater(
            wrong.send(['PING']),
            throwsA(isA<RedisException>()
                .having((e) => e.message, 'message', startsWith('connect'))));
      }
    });

    test('an unreachable server fails fast with RedisException', () async {
      final port = await _unusedPort();
      final dead = GisilaRedis(port: port);
      addTearDown(dead.close);
      await expectLater(dead.connect(), throwsA(isA<RedisException>()));
    });

    redisTest('error replies keep the connection', () async {
      await expectLater(redis.send(['NOSUCHCOMMAND']),
          throwsA(isA<RedisException>()));
      expect(await redis.send(['PING']), 'PONG');
    });
  });

  group('RedisRateLimitStore', () {
    redisTest('a bucket refuses once empty and says how long to wait',
        () async {
      final store = RedisRateLimitStore(redis);
      for (var i = 0; i < 3; i++) {
        expect(await store.take('k', capacity: 3, refillPerSecond: 1), isNull);
      }
      final wait = await store.take('k', capacity: 3, refillPerSecond: 1);
      expect(wait, isNotNull);
      expect(wait!.inMilliseconds, inInclusiveRange(1, 1000));
      final ttl = await redis.send(['PTTL', 'gisila:rl:k']) as int;
      expect(ttl, inInclusiveRange(1, 4000));
    });

    redisTest('stores on different connections share one budget', () async {
      final other = GisilaRedis(port: _port);
      addTearDown(other.close);
      final a = RedisRateLimitStore(redis);
      final b = RedisRateLimitStore(other);
      expect(await a.take('shared', capacity: 2, refillPerSecond: 0.01),
          isNull);
      expect(await b.take('shared', capacity: 2, refillPerSecond: 0.01),
          isNull);
      expect(await a.take('shared', capacity: 2, refillPerSecond: 0.01),
          isNotNull);
      expect(await b.take('elsewhere', capacity: 2, refillPerSecond: 0.01),
          isNull);
    });

    redisTest('reloads the script after SCRIPT FLUSH', () async {
      final store = RedisRateLimitStore(redis);
      expect(await store.take('s', capacity: 5, refillPerSecond: 1), isNull);
      await redis.send(['SCRIPT', 'FLUSH']);
      expect(await store.take('s', capacity: 5, refillPerSecond: 1), isNull);
    });

    test('fails open by default and closed on request', () async {
      final dead = GisilaRedis(port: await _unusedPort());
      addTearDown(dead.close);
      final errors = <Object>[];
      final open = RedisRateLimitStore(dead, onError: errors.add);
      expect(await open.take('x', capacity: 1, refillPerSecond: 1), isNull);
      expect(errors.single, isA<RedisException>());
      final closed = RedisRateLimitStore(dead, failOpen: false);
      expect(await closed.take('x', capacity: 1, refillPerSecond: 1),
          const Duration(seconds: 1));
    });

    redisTest('two apps (as two isolates) share the app-wide limit', () async {
      Future<Handler> app() async {
        final a = GisilaApp(
            config: AppConfig(
          logRequests: false,
          rateLimit: const RateLimitConfig(requestsPerMinute: 2),
          rateLimitStore: RedisRateLimitStore(redis),
        ));
        a.registerController(attacher: (_, router, {prefix = ''}) {
          router.get('/ping', (Request r) => Response.ok('pong'));
        });
        return a.buildHandler();
      }

      final one = await app();
      final two = await app();
      Future<int> hit(Handler h) async =>
          (await h(Request('GET', Uri.parse('http://x/ping')))).statusCode;
      expect(await hit(one), 200);
      expect(await hit(two), 200);
      expect(await hit(one), 429);
    });
  });

  group('RedisCacheProvider', () {
    late RedisCacheProvider cache;
    setUp(() => cache = RedisCacheProvider(redis));

    redisTest('get, set with ttl, delete, exists', () async {
      await cache.set('a', '1');
      await cache.set('b', '2', ttl: const Duration(seconds: 30));
      expect(await cache.get('a'), '1');
      expect(await cache.get('missing'), isNull);
      expect(await redis.send(['PTTL', 'gisila:cache:b']) as int,
          inInclusiveRange(1, 30000));
      expect(await cache.exists('a'), isTrue);
      expect(await cache.delete('a'), isTrue);
      expect(await cache.delete('a'), isFalse);
      expect(await cache.exists('a'), isFalse);
    });

    redisTest('expired entries are gone', () async {
      await cache.set('t', 'x', ttl: const Duration(milliseconds: 50));
      await Future<void>.delayed(const Duration(milliseconds: 150));
      expect(await cache.get('t'), isNull);
    });

    redisTest('keys and flush stay inside the prefix', () async {
      await redis.send(['SET', 'someone-else', 'keep']);
      final other = RedisCacheProvider(redis, prefix: 'app2:');
      await other.set('user:1', 'other app');
      await cache.mset({'user:1': 'a', 'user:2': 'b', 'order:1': 'c'});
      expect((await cache.keys('user:*'))..sort(), ['user:1', 'user:2']);
      await cache.flush();
      expect(await cache.keys('*'), isEmpty);
      expect(await redis.send(['GET', 'someone-else']), 'keep');
      expect(await other.get('user:1'), 'other app');
    });

    redisTest('counters, mget and mdelete', () async {
      expect(await cache.increment('n'), 1);
      expect(await cache.increment('n', by: 5), 6);
      expect(await cache.decrement('n', by: 2), 4);
      await cache.set('s', 'text');
      await expectLater(cache.increment('s'), throwsFormatException);
      expect(await cache.mget(['n', 's', 'missing']), {'n': '4', 's': 'text'});
      expect(await cache.mdelete(['n', 's', 'missing']), 2);
      expect(await cache.mget([]), isEmpty);
    });

    redisTest('connects at start and closes at stop', () async {
      final own = GisilaRedis(port: _port);
      final app = GisilaApp(
          config: AppConfig(logRequests: false, cache: RedisCacheProvider(own)));
      await app.start(host: '127.0.0.1', port: 0);
      await app.stop();
      await expectLater(own.send(['PING']), throwsA(isA<RedisException>()));
    });
  });
}
