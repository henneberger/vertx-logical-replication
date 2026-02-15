package dev.henneberger.vertx.sqlserver.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SqlServerReplicationAppConfigTest {

  @Test
  void mapsEnvironmentAndBuildsOptions() {
    Map<String, String> env = new HashMap<>();
    env.put("SQLSERVER_HOST", "sql.internal");
    env.put("SQLSERVER_PORT", "11433");
    env.put("SQLSERVER_DATABASE", "app");
    env.put("SQLSERVER_USER", "service");
    env.put("SQLSERVER_PASSWORD_ENV", "APP_DB_PASSWORD");
    env.put("SQLSERVER_SSL", "true");
    env.put("HTTP_PORT", "9090");

    SqlServerReplicationAppConfig cfg = SqlServerReplicationAppConfig.fromMap(env, "dbo_orders");

    assertEquals("sql.internal", cfg.host());
    assertEquals(11433, cfg.port());
    assertEquals("app", cfg.database());
    assertEquals("service", cfg.user());
    assertEquals("APP_DB_PASSWORD", cfg.passwordEnv());
    assertTrue(cfg.ssl());
    assertEquals(9090, cfg.httpPort());

    SqlServerReplicationOptions options = cfg.toReplicationOptions();
    assertEquals("sql.internal", options.getHost());
    assertEquals(11433, options.getPort());
    assertEquals("dbo_orders", options.getCaptureInstance());
    assertEquals("APP_DB_PASSWORD", options.getPasswordEnv());
  }
}
