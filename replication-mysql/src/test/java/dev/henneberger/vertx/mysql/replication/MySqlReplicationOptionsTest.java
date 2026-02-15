package dev.henneberger.vertx.mysql.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

class MySqlReplicationOptionsTest {

  @Test
  void readsAndWritesJson() {
    MySqlReplicationOptions options = new MySqlReplicationOptions(new JsonObject()
      .put("host", "mysql.internal")
      .put("port", 13306)
      .put("database", "app")
      .put("user", "replicator")
      .put("serverId", 42L)
      .put("connectTimeoutMs", 5000L)
      .put("maxConcurrentDispatch", 2));

    assertEquals("mysql.internal", options.getHost());
    assertEquals(42L, options.getServerId());
    assertEquals(2, options.getMaxConcurrentDispatch());

    JsonObject json = options.toJson();
    assertEquals("mysql.internal", json.getString("host"));
    assertEquals(13306, json.getInteger("port"));
  }

  @Test
  void validatesRequiredFields() {
    MySqlReplicationOptions options = new MySqlReplicationOptions()
      .setHost("localhost")
      .setPort(3306)
      .setDatabase("db")
      .setUser("user")
      .setServerId(1L)
      .setConnectTimeoutMs(1000L);

    options.validate();
    assertThrows(IllegalArgumentException.class,
      () -> options.setServerId(0L).validate());
  }
}
