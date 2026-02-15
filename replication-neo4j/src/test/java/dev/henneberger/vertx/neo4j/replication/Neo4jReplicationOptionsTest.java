package dev.henneberger.vertx.neo4j.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

class Neo4jReplicationOptionsTest {

  @Test
  void readsAndSerializesJson() {
    Neo4jReplicationOptions options = new Neo4jReplicationOptions(new JsonObject()
      .put("uri", "neo4j://neo.internal:7687")
      .put("database", "neo4j")
      .put("user", "neo4j")
      .put("sourceName", "neo_events")
      .put("eventQuery", "MATCH (n) RETURN 'neo_events' AS source, 'UPDATE' AS operation, {} AS before, {} AS after, '1' AS position, datetime() AS commitTimestamp LIMIT $limit")
      .put("pollIntervalMs", 750)
      .put("batchSize", 250)
      .put("maxConcurrentDispatch", 2));

    assertEquals("neo4j://neo.internal:7687", options.getUri());
    assertEquals("neo4j", options.getDatabase());
    assertEquals("neo_events", options.getSourceName());
    assertEquals(750, options.getPollIntervalMs());

    JsonObject json = options.toJson();
    assertEquals("neo_events", json.getString("sourceName"));
    assertEquals(2, json.getInteger("maxConcurrentDispatch"));
  }

  @Test
  void validatesRequiredFields() {
    Neo4jReplicationOptions options = new Neo4jReplicationOptions()
      .setUri("neo4j://localhost:7687")
      .setDatabase("neo4j")
      .setUser("neo4j")
      .setSourceName("neo_events")
      .setEventQuery("RETURN 'neo_events' AS source, 'UPDATE' AS operation, {} AS before, {} AS after, '1' AS position, datetime() AS commitTimestamp LIMIT $limit");

    options.validate();
    assertTrue(options.isAutoStart());

    assertThrows(IllegalArgumentException.class, () -> options.setBatchSize(0).validate());
  }
}
