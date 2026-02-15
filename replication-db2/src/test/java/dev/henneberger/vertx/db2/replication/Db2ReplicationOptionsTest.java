package dev.henneberger.vertx.db2.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

class Db2ReplicationOptionsTest {

  @Test
  void readsAndSerializesJson() {
    Db2ReplicationOptions options = new Db2ReplicationOptions(new JsonObject()
      .put("host", "db2.internal")
      .put("port", 50001)
      .put("database", "app")
      .put("user", "db2inst1")
      .put("passwordEnv", "DB2_PASSWORD")
      .put("sourceTable", "CDC_ORDERS")
      .put("pollIntervalMs", 750)
      .put("batchSize", 250)
      .put("maxConcurrentDispatch", 2));

    assertEquals("db2.internal", options.getHost());
    assertEquals(50001, options.getPort());
    assertEquals("CDC_ORDERS", options.getSourceTable());
    assertEquals(750, options.getPollIntervalMs());

    JsonObject json = options.toJson();
    assertEquals("db2.internal", json.getString("host"));
    assertEquals(2, json.getInteger("maxConcurrentDispatch"));
  }

  @Test
  void validatesRequiredFields() {
    Db2ReplicationOptions options = new Db2ReplicationOptions()
      .setHost("localhost")
      .setPort(50000)
      .setDatabase("db")
      .setUser("user")
      .setSourceTable("CDC_ORDERS");

    options.validate();
    assertTrue(options.isAutoStart());

    assertThrows(IllegalArgumentException.class, () -> options.setBatchSize(0).validate());
  }
}
