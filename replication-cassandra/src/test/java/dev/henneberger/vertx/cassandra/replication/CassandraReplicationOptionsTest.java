package dev.henneberger.vertx.cassandra.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

class CassandraReplicationOptionsTest {

  @Test
  void readsAndSerializesJson() {
    CassandraReplicationOptions options = new CassandraReplicationOptions(new JsonObject()
      .put("host", "cass.internal")
      .put("port", 9142)
      .put("localDatacenter", "dc1")
      .put("keyspace", "app")
      .put("sourceTable", "events")
      .put("user", "cassandra")
      .put("pollIntervalMs", 750)
      .put("batchSize", 250)
      .put("maxConcurrentDispatch", 2));

    assertEquals("cass.internal", options.getHost());
    assertEquals(9142, options.getPort());
    assertEquals("events", options.getSourceTable());
    assertEquals(750, options.getPollIntervalMs());

    JsonObject json = options.toJson();
    assertEquals("cass.internal", json.getString("host"));
    assertEquals(2, json.getInteger("maxConcurrentDispatch"));
  }

  @Test
  void validatesRequiredFields() {
    CassandraReplicationOptions options = new CassandraReplicationOptions()
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
