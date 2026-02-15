package dev.henneberger.vertx.scylladb.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ScyllaDbLogicalReplicationStreamHelpersTest {

  @Test
  void filterMatchesBySourceAndOperation() {
    ScyllaDbChangeEvent event = new ScyllaDbChangeEvent(
      "events",
      ScyllaDbChangeEvent.Operation.INSERT,
      Map.of(),
      Map.of("id", 101),
      "10",
      Instant.parse("2026-02-15T20:00:00Z"),
      Map.of());

    ScyllaDbChangeFilter filter = ScyllaDbChangeFilter.sources("events")
      .operations(ScyllaDbChangeEvent.Operation.INSERT);

    assertTrue(filter.test(event));
    assertFalse(ScyllaDbChangeFilter.sources("other").test(event));
    assertFalse(ScyllaDbChangeFilter.all().operations(ScyllaDbChangeEvent.Operation.DELETE).test(event));
  }

  @Test
  void eventAfterJsonContainsMappedFields() {
    ScyllaDbChangeEvent event = new ScyllaDbChangeEvent(
      "events",
      ScyllaDbChangeEvent.Operation.UPDATE,
      Map.of("id", 101),
      Map.of("id", 101, "amount", 55.25),
      "11",
      Instant.parse("2026-02-15T20:00:01Z"),
      Map.of("adapter", "scylladb"));

    assertEquals(101, event.afterJson().getInteger("id"));
    assertEquals(55.25, event.afterJson().getDouble("amount"));
  }
}
