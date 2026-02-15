package dev.henneberger.vertx.mariadb.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

class MariaDbReplicationOptionsTest {

  @Test
  void readsAndSerializesJson() {
    MariaDbReplicationOptions options = new MariaDbReplicationOptions(new JsonObject()
      .put("host", "mariadb.internal")
      .put("port", 13306)
      .put("database", "app")
      .put("user", "replicator")
      .put("passwordEnv", "MARIADB_PASSWORD")
      .put("sourceTable", "cdc_orders")
      .put("positionColumn", "position")
      .put("operationColumn", "operation")
      .put("beforeColumn", "before_json")
      .put("afterColumn", "after_json")
      .put("commitTimestampColumn", "commit_ts")
      .put("pollIntervalMs", 750)
      .put("batchSize", 250)
      .put("maxConcurrentDispatch", 2));

    assertEquals("mariadb.internal", options.getHost());
    assertEquals(13306, options.getPort());
    assertEquals("cdc_orders", options.getSourceTable());
    assertEquals(750, options.getPollIntervalMs());
    assertEquals(250, options.getBatchSize());

    JsonObject json = options.toJson();
    assertEquals("mariadb.internal", json.getString("host"));
    assertEquals(2, json.getInteger("maxConcurrentDispatch"));
  }

  @Test
  void validatesRequiredFields() {
    MariaDbReplicationOptions options = new MariaDbReplicationOptions()
      .setHost("localhost")
      .setPort(3306)
      .setDatabase("db")
      .setUser("user")
      .setSourceTable("cdc_orders");

    options.validate();
    assertTrue(options.isAutoStart());

    assertThrows(IllegalArgumentException.class,
      () -> options.setMaxConcurrentDispatch(0).validate());
  }
}
