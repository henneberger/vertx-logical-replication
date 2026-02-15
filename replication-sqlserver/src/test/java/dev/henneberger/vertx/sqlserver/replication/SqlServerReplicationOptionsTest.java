package dev.henneberger.vertx.sqlserver.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

class SqlServerReplicationOptionsTest {

  @Test
  void readsAndSerializesJson() {
    SqlServerReplicationOptions options = new SqlServerReplicationOptions(new JsonObject()
      .put("host", "sql.internal")
      .put("port", 11433)
      .put("database", "app")
      .put("user", "sa")
      .put("passwordEnv", "MSSQL_PASSWORD")
      .put("captureInstance", "dbo_orders")
      .put("pollIntervalMs", 750)
      .put("maxBatchSize", 500)
      .put("preflightMode", "wait-until-ready")
      .put("preflightMaxWaitMs", 20000)
      .put("preflightRetryIntervalMs", 300)
      .put("valueNormalizationMode", "raw")
      .put("maxConcurrentDispatch", 2)
      .put("ssl", false));

    assertEquals("sql.internal", options.getHost());
    assertEquals(11433, options.getPort());
    assertEquals("dbo_orders", options.getCaptureInstance());
    assertEquals(750, options.getPollIntervalMs());
    assertEquals("wait-until-ready", options.getPreflightMode());
    assertEquals("RAW", options.getValueNormalizationMode());

    JsonObject json = options.toJson();
    assertEquals("sql.internal", json.getString("host"));
    assertEquals(2, json.getInteger("maxConcurrentDispatch"));
    assertEquals("wait-until-ready", json.getString("preflightMode"));
    assertEquals("RAW", json.getString("valueNormalizationMode"));
  }

  @Test
  void validatesRequiredFields() {
    SqlServerReplicationOptions options = new SqlServerReplicationOptions()
      .setHost("localhost")
      .setPort(1433)
      .setDatabase("db")
      .setUser("user")
      .setCaptureInstance("ci");

    options.validate();
    assertTrue(options.isAutoStart());

    assertThrows(IllegalArgumentException.class,
      () -> options.setMaxConcurrentDispatch(0).validate());
  }
}
