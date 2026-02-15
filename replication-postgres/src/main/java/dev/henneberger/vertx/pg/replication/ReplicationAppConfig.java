/*
 * Copyright (C) 2026 Daniel Henneberger
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.henneberger.vertx.pg.replication;

import java.util.Map;
import java.util.Objects;

public final class ReplicationAppConfig {

  private static final int DEFAULT_HTTP_PORT = 8080;

  private final String pgHost;
  private final int pgPort;
  private final String pgDatabase;
  private final String pgUser;
  private final String pgPasswordEnv;
  private final boolean ssl;
  private final int httpPort;
  private final String slotName;

  private ReplicationAppConfig(String pgHost,
                               int pgPort,
                               String pgDatabase,
                               String pgUser,
                               String pgPasswordEnv,
                               boolean ssl,
                               int httpPort,
                               String slotName) {
    this.pgHost = pgHost;
    this.pgPort = pgPort;
    this.pgDatabase = pgDatabase;
    this.pgUser = pgUser;
    this.pgPasswordEnv = pgPasswordEnv;
    this.ssl = ssl;
    this.httpPort = httpPort;
    this.slotName = slotName;
  }

  public static ReplicationAppConfig fromEnv(String slotName) {
    return fromMap(System.getenv(), slotName);
  }

  static ReplicationAppConfig fromMap(Map<String, String> env, String slotName) {
    Objects.requireNonNull(env, "env");
    Objects.requireNonNull(slotName, "slotName");

    String host = envOrDefault(env, "PGHOST", PostgresReplicationOptions.DEFAULT_HOST);
    int port = intEnvOrDefault(env, "PGPORT", PostgresReplicationOptions.DEFAULT_PORT);
    String database = envOrDefault(env, "PGDATABASE", "postgres");
    String user = envOrDefault(env, "PGUSER", "postgres");
    String passwordEnv = envOrDefault(env, "PG_PASSWORD_ENV", "PGPASSWORD");
    boolean ssl = boolEnvOrDefault(env, "PGSSL", false);
    int httpPort = intEnvOrDefault(env, "HTTP_PORT", DEFAULT_HTTP_PORT);

    return new ReplicationAppConfig(host, port, database, user, passwordEnv, ssl, httpPort, slotName);
  }

  public String pgHost() {
    return pgHost;
  }

  public int pgPort() {
    return pgPort;
  }

  public String pgDatabase() {
    return pgDatabase;
  }

  public String pgUser() {
    return pgUser;
  }

  public String pgPasswordEnv() {
    return pgPasswordEnv;
  }

  public boolean ssl() {
    return ssl;
  }

  public int httpPort() {
    return httpPort;
  }

  public PostgresReplicationOptions toReplicationOptions() {
    return new PostgresReplicationOptions()
      .setHost(pgHost)
      .setPort(pgPort)
      .setDatabase(pgDatabase)
      .setUser(pgUser)
      .setPasswordEnv(pgPasswordEnv)
      .setSsl(ssl)
      .setSlotName(slotName);
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
