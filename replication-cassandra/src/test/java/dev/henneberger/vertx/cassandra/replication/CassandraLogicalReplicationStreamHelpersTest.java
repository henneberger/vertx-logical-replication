package dev.henneberger.vertx.cassandra.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CassandraLogicalReplicationStreamHelpersTest {

  @Test
  void filterMatchesBySourceAndOperation() {
    CassandraChangeEvent event = new CassandraChangeEvent(
      "events",
      CassandraChangeEvent.Operation.INSERT,
      Map.of(),
      Map.of("id", 101),
      "10",
      Instant.parse("2026-02-15T20:00:00Z"),
      Map.of());

    CassandraChangeFilter filter = CassandraChangeFilter.sources("events")
      .operations(CassandraChangeEvent.Operation.INSERT);

    assertTrue(filter.test(event));
    assertFalse(CassandraChangeFilter.sources("other").test(event));
    assertFalse(CassandraChangeFilter.all().operations(CassandraChangeEvent.Operation.DELETE).test(event));
  }

  @Test
  void eventAfterJsonContainsMappedFields() {
    CassandraChangeEvent event = new CassandraChangeEvent(
      "events",
      CassandraChangeEvent.Operation.UPDATE,
      Map.of("id", 101),
      Map.of("id", 101, "amount", 55.25),
      "11",
      Instant.parse("2026-02-15T20:00:01Z"),
      Map.of("adapter", "cassandra"));

    assertEquals(101, event.afterJson().getInteger("id"));
    assertEquals(55.25, event.afterJson().getDouble("amount"));
  }
}
