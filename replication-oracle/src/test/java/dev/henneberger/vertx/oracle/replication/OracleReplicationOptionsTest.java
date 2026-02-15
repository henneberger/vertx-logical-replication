package dev.henneberger.vertx.oracle.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

class OracleReplicationOptionsTest {

  @Test
  void readsAndSerializesJson() {
    OracleReplicationOptions options = new OracleReplicationOptions(new JsonObject()
      .put("host", "oracle.internal")
      .put("port", 1522)
      .put("database", "ORCLPDB1")
      .put("user", "system")
      .put("passwordEnv", "ORACLE_PASSWORD")
      .put("sourceTable", "CDC_ORDERS")
      .put("pollIntervalMs", 750)
      .put("batchSize", 250)
      .put("maxConcurrentDispatch", 2));

    assertEquals("oracle.internal", options.getHost());
    assertEquals(1522, options.getPort());
    assertEquals("CDC_ORDERS", options.getSourceTable());
    assertEquals(750, options.getPollIntervalMs());

    JsonObject json = options.toJson();
    assertEquals("oracle.internal", json.getString("host"));
    assertEquals(2, json.getInteger("maxConcurrentDispatch"));
  }

  @Test
  void validatesRequiredFields() {
    OracleReplicationOptions options = new OracleReplicationOptions()
      .setHost("localhost")
      .setPort(1521)
      .setDatabase("ORCLCDB")
      .setUser("user")
      .setSourceTable("CDC_ORDERS");

    options.validate();
    assertTrue(options.isAutoStart());

    assertThrows(IllegalArgumentException.class, () -> options.setBatchSize(0).validate());
  }
}
