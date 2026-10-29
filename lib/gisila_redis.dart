/// Redis-backed stores for gisila: [RedisRateLimitStore] shares rate limits
/// and [RedisCacheProvider] shares the cache across isolates and servers.
///
/// ```dart
/// import 'package:gisila/gisila.dart';
/// import 'package:gisila_redis/gisila_redis.dart';
///
/// final redis = GisilaRedis.fromUrl(Platform.environment['REDIS_URL']!);
/// final app = GisilaApp(
///   config: AppConfig(
///     rateLimit: const RateLimitConfig(requestsPerMinute: 300),
///     rateLimitStore: RedisRateLimitStore(redis),
///     // Connects at start() and closes the client at stop().
///     cache: RedisCacheProvider(redis),
///   ),
/// );
/// ```
library gisila_redis;

export 'src/cache_provider.dart';
export 'src/client.dart';
export 'src/rate_limit_store.dart';
