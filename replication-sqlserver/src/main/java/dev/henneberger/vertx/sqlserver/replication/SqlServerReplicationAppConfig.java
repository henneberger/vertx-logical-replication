package dev.henneberger.vertx.sqlserver.replication;

import java.util.Map;
import java.util.Objects;

public final class SqlServerReplicationAppConfig {

  private static final int DEFAULT_HTTP_PORT = 8080;

  private final String host;
  private final int port;
  private final String database;
  private final String user;
  private final String passwordEnv;
  private final boolean ssl;
  private final int httpPort;
  private final String captureInstance;

  private SqlServerReplicationAppConfig(String host,
                                        int port,
                                        String database,
                                        String user,
                                        String passwordEnv,
                                        boolean ssl,
                                        int httpPort,
                                        String captureInstance) {
    this.host = host;
    this.port = port;
    this.database = database;
    this.user = user;
    this.passwordEnv = passwordEnv;
    this.ssl = ssl;
    this.httpPort = httpPort;
    this.captureInstance = captureInstance;
  }

  public static SqlServerReplicationAppConfig fromEnv(String captureInstance) {
    return fromMap(System.getenv(), captureInstance);
  }

  static SqlServerReplicationAppConfig fromMap(Map<String, String> env, String captureInstance) {
    Objects.requireNonNull(env, "env");
    Objects.requireNonNull(captureInstance, "captureInstance");

    String host = envOrDefault(env, "SQLSERVER_HOST", SqlServerReplicationOptions.DEFAULT_HOST);
    int port = intEnvOrDefault(env, "SQLSERVER_PORT", SqlServerReplicationOptions.DEFAULT_PORT);
    String database = envOrDefault(env, "SQLSERVER_DATABASE", "master");
    String user = envOrDefault(env, "SQLSERVER_USER", "sa");
    String passwordEnv = envOrDefault(env, "SQLSERVER_PASSWORD_ENV", "SQLSERVER_PASSWORD");
    boolean ssl = boolEnvOrDefault(env, "SQLSERVER_SSL", true);
    int httpPort = intEnvOrDefault(env, "HTTP_PORT", DEFAULT_HTTP_PORT);

    return new SqlServerReplicationAppConfig(host, port, database, user, passwordEnv, ssl, httpPort, captureInstance);
  }

  public String host() {
    return host;
  }

  public int port() {
    return port;
  }

  public String database() {
    return database;
  }

  public String user() {
    return user;
  }

  public String passwordEnv() {
    return passwordEnv;
  }

  public boolean ssl() {
    return ssl;
  }

  public int httpPort() {
    return httpPort;
  }

  public SqlServerReplicationOptions toReplicationOptions() {
    return new SqlServerReplicationOptions()
      .setHost(host)
      .setPort(port)
      .setDatabase(database)
      .setUser(user)
      .setPasswordEnv(passwordEnv)
      .setSsl(ssl)
      .setCaptureInstance(captureInstance);
  }

  private static String envOrDefault(Map<String, String> env, String key, String defaultValue) {
    String value = env.get(key);
    return value == null || value.isBlank() ? defaultValue : value;
  }

  private static int intEnvOrDefault(Map<String, String> env, String key, int defaultValue) {
    String value = env.get(key);
    if (value == null || value.isBlank()) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException ignore) {
      return defaultValue;
    }
  }

  private static boolean boolEnvOrDefault(Map<String, String> env, String key, boolean defaultValue) {
    String value = env.get(key);
    if (value == null || value.isBlank()) {
      return defaultValue;
    }
    return "true".equalsIgnoreCase(value) || "1".equals(value) || "yes".equalsIgnoreCase(value);
  }
}
