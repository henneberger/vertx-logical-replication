package dev.henneberger.vertx.scylladb.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

class ScyllaDbReplicationOptionsTest {

  @Test
  void readsAndSerializesJson() {
    ScyllaDbReplicationOptions options = new ScyllaDbReplicationOptions(new JsonObject()
      .put("host", "scylla.internal")
      .put("port", 9142)
      .put("localDatacenter", "dc1")
      .put("keyspace", "app")
      .put("sourceTable", "events")
      .put("user", "scylla")
      .put("pollIntervalMs", 750)
      .put("batchSize", 250)
      .put("maxConcurrentDispatch", 2));

    assertEquals("scylla.internal", options.getHost());
    assertEquals(9142, options.getPort());
    assertEquals("events", options.getSourceTable());
    assertEquals(750, options.getPollIntervalMs());

    JsonObject json = options.toJson();
    assertEquals("scylla.internal", json.getString("host"));
    assertEquals(2, json.getInteger("maxConcurrentDispatch"));
  }

  @Test
  void validatesRequiredFields() {
    ScyllaDbReplicationOptions options = new ScyllaDbReplicationOptions()
      .setHost("localhost")
      .setPort(9042)
      .setLocalDatacenter("dc1")
      .setKeyspace("ks")
      .setSourceTable("events")
      .setUser("user");

    options.validate();
    assertTrue(options.isAutoStart());

    assertThrows(IllegalArgumentException.class, () -> options.setBatchSize(0).validate());
  }
}
