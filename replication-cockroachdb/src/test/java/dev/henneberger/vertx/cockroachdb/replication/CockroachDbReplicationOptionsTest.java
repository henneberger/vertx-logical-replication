package dev.henneberger.vertx.cockroachdb.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.json.JsonObject;
import java.util.List;
import org.junit.jupiter.api.Test;

class CockroachDbReplicationOptionsTest {

  @Test
  void readsAndSerializesJson() {
    CockroachDbReplicationOptions options = new CockroachDbReplicationOptions(new JsonObject()
      .put("host", "roach.internal")
      .put("port", 26257)
      .put("database", "app")
      .put("user", "root")
      .put("passwordEnv", "COCKROACH_PASSWORD")
      .put("sourceTable", "cdc_orders")
      .put("initialCursor", "2026-02-16T00:00:00Z")
      .put("changefeedOptions", new JsonObject().put("resolved", "5s"))
      .put("cliCommand", List.of("cockroach", "sql", "--insecure", "--format=csv", "-e"))
      .put("positionColumn", "position")
      .put("operationColumn", "operation")
      .put("pollIntervalMs", 750)
      .put("batchSize", 250)
      .put("maxConcurrentDispatch", 2));

    assertEquals("roach.internal", options.getHost());
    assertEquals(26257, options.getPort());
    assertEquals("cdc_orders", options.getSourceTable());
    assertEquals("2026-02-16T00:00:00Z", options.getInitialCursor());
    assertEquals("5s", options.getChangefeedOptions().get("resolved"));
    assertEquals("cockroach", options.getCliCommand().get(0));
    assertEquals(750, options.getPollIntervalMs());

    JsonObject json = options.toJson();
    assertEquals("roach.internal", json.getString("host"));
    assertEquals("2026-02-16T00:00:00Z", json.getString("initialCursor"));
    assertEquals("cockroach", json.getJsonArray("cliCommand").getString(0));
    assertEquals(2, json.getInteger("maxConcurrentDispatch"));
  }

  @Test
  void validatesRequiredFields() {
    CockroachDbReplicationOptions options = new CockroachDbReplicationOptions()
      .setHost("localhost")
      .setPort(26257)
      .setDatabase("db")
      .setUser("root")
      .setSourceTable("cdc_orders");

    options.validate();
    assertTrue(options.isAutoStart());

    assertThrows(IllegalArgumentException.class,
      () -> options.setMaxConcurrentDispatch(0).validate());
  }
}
