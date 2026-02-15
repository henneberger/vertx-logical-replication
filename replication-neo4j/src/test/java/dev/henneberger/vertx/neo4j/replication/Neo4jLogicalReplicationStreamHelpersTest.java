package dev.henneberger.vertx.neo4j.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class Neo4jLogicalReplicationStreamHelpersTest {

  @Test
  void filterMatchesBySourceAndOperation() {
    Neo4jChangeEvent event = new Neo4jChangeEvent(
      "neo_events",
      Neo4jChangeEvent.Operation.INSERT,
      Map.of(),
      Map.of("id", 101),
      "10",
      Instant.parse("2026-02-15T20:00:00Z"),
      Map.of());

    Neo4jChangeFilter filter = Neo4jChangeFilter.sources("neo_events")
      .operations(Neo4jChangeEvent.Operation.INSERT);

    assertTrue(filter.test(event));
    assertFalse(Neo4jChangeFilter.sources("other").test(event));
    assertFalse(Neo4jChangeFilter.all().operations(Neo4jChangeEvent.Operation.DELETE).test(event));
  }

  @Test
  void eventAfterJsonContainsMappedFields() {
    Neo4jChangeEvent event = new Neo4jChangeEvent(
      "neo_events",
      Neo4jChangeEvent.Operation.UPDATE,
      Map.of("id", 101),
      Map.of("id", 101, "amount", 55.25),
      "11",
      Instant.parse("2026-02-15T20:00:01Z"),
      Map.of("adapter", "neo4j"));

    assertEquals(101, event.afterJson().getInteger("id"));
    assertEquals(55.25, event.afterJson().getDouble("amount"));
  }
}
