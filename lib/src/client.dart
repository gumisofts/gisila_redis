/// A small Redis client for the stores: one pipelined connection, opened
/// lazily, re-opened after a failure, with a timeout on every command.
library gisila_redis.client;

import 'dart:async';
import 'dart:io';

import 'package:redis/redis.dart';

/// A failed Redis command: an error reply, a timeout, or a lost
/// connection.
class RedisException implements Exception {
  RedisException(this.message);
  final String message;

  @override
  String toString() => 'RedisException: $message';
}

/// Connection to one Redis server.
///
/// Commands share one connection (Redis pipelines them). The connection
/// opens on first use; after a timeout or a socket error it is dropped
/// and the next command opens a new one, so a Redis restart heals on its
/// own.
class GisilaRedis {
  GisilaRedis({
    this.host = 'localhost',
    this.port = 6379,
    this.username,
    this.password,
    this.database = 0,
    this.tls = false,
    this.securityContext,
    this.connectTimeout = const Duration(seconds: 5),
    this.commandTimeout = const Duration(seconds: 2),
  });

  /// From `redis://[user[:password]@]host[:port][/db]`, or `rediss://`
  /// for TLS. A lone credential is the password, as with `redis-cli -u`.
  factory GisilaRedis.fromUrl(
    String url, {
    SecurityContext? securityContext,
    Duration connectTimeout = const Duration(seconds: 5),
    Duration commandTimeout = const Duration(seconds: 2),
  }) {
    final uri = Uri.parse(url);
    if (uri.scheme != 'redis' && uri.scheme != 'rediss') {
      throw FormatException('Not a redis:// or rediss:// URL', url);
    }
    String? username;
    String? password;
    if (uri.userInfo.isNotEmpty) {
      final i = uri.userInfo.indexOf(':');
      if (i < 0) {
        password = Uri.decodeComponent(uri.userInfo);
      } else {
        final user = Uri.decodeComponent(uri.userInfo.substring(0, i));
        username = user.isEmpty ? null : user;
        password = Uri.decodeComponent(uri.userInfo.substring(i + 1));
      }
    }
    final path = uri.path.replaceAll('/', '');
    return GisilaRedis(
      host: uri.host.isEmpty ? 'localhost' : uri.host,
      port: uri.hasPort ? uri.port : 6379,
      username: username,
      password: password,
      database: path.isEmpty ? 0 : int.parse(path),
      tls: uri.scheme == 'rediss',
      securityContext: securityContext,
      connectTimeout: connectTimeout,
      commandTimeout: commandTimeout,
    );
  }

  final String host;
  final int port;
  final String? username;
  final String? password;
  final int database;
  final bool tls;
  final SecurityContext? securityContext;
  final Duration connectTimeout;

  /// How long a command may take before it fails and the connection is
  /// replaced. Keep it short: rate limiting runs on every request.
  final Duration commandTimeout;

  Future<(RedisConnection, Command)>? _connecting;
  bool _closed = false;

  /// Sends one command (`['SET', key, value]`) and returns the reply.
  /// Throws [RedisException].
  Future<Object?> send(List<Object> command) async {
    if (_closed) throw RedisException('client is closed');
    final connecting = _connecting ??= _open();
    final Command cmd;
    try {
      (_, cmd) = await connecting;
    } catch (e) {
      // Try a fresh connection next time.
      if (identical(_connecting, connecting)) _connecting = null;
      throw e is RedisException ? e : RedisException('connect: $e');
    }
    try {
      return await cmd.send_object(command).timeout(commandTimeout);
    } on RedisError catch (e) {
      // An error reply: the connection itself is fine.
      throw RedisException(e.error);
    } on TimeoutException {
      await _drop(connecting);
      throw RedisException('${command.first} timed out after '
          '${commandTimeout.inMilliseconds} ms');
    } catch (e) {
      await _drop(connecting);
      throw RedisException('${command.first} failed: $e');
    }
  }

  /// Opens the connection now, so a wrong address or password fails at
  /// start-up rather than on the first request.
  Future<void> connect() async {
    await send(const ['PING']);
  }

  /// Closes the connection. The client can't be used afterwards.
  Future<void> close() async {
    _closed = true;
    final connecting = _connecting;
    _connecting = null;
    if (connecting != null) await _closeQuietly(connecting);
  }

  Future<(RedisConnection, Command)> _open() async {
    final Socket socket = tls
        ? await SecureSocket.connect(host, port,
            context: securityContext, timeout: connectTimeout)
        : await Socket.connect(host, port, timeout: connectTimeout);
    final conn = RedisConnection();
    final cmd = await conn.connectWithSocket(socket);
    try {
      if (password != null) {
        await cmd
            .send_object(
                ['AUTH', if (username != null) username!, password!])
            .timeout(commandTimeout);
      }
      if (database != 0) {
        await cmd.send_object(['SELECT', '$database']).timeout(commandTimeout);
      }
    } catch (e) {
      socket.destroy();
      throw RedisException(
          e is RedisError ? 'connect: ${e.error}' : 'connect: $e');
    }
    return (conn, cmd);
  }

  Future<void> _drop(Future<(RedisConnection, Command)> connecting) async {
    if (identical(_connecting, connecting)) _connecting = null;
    await _closeQuietly(connecting);
  }

  static Future<void> _closeQuietly(
      Future<(RedisConnection, Command)> connecting) async {
    try {
      final (conn, _) = await connecting;
      await conn.close().timeout(const Duration(seconds: 1));
    } catch (_) {
      // Already broken or never opened.
    }
  }
}
