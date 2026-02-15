package dev.henneberger.vertx.cockroachdb.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CockroachDbLogicalReplicationStreamHelpersTest {

  @Test
  void filterMatchesBySourceAndOperation() {
    CockroachDbChangeEvent event = new CockroachDbChangeEvent(
      "cdc_orders",
      CockroachDbChangeEvent.Operation.UPDATE,
      Map.of("id", 101),
      Map.of("id", 101, "amount", 75.25),
      "44",
      Instant.parse("2026-02-15T20:00:00Z"),
      Map.of());

    CockroachDbChangeFilter filter = CockroachDbChangeFilter.sources("cdc_orders")
      .operations(CockroachDbChangeEvent.Operation.UPDATE);

    assertTrue(filter.test(event));
    assertFalse(CockroachDbChangeFilter.sources("other_table").test(event));
    assertFalse(CockroachDbChangeFilter.all().operations(CockroachDbChangeEvent.Operation.INSERT).test(event));
  }

  @Test
  void eventAfterJsonContainsMappedFields() {
    CockroachDbChangeEvent event = new CockroachDbChangeEvent(
      "cdc_orders",
      CockroachDbChangeEvent.Operation.INSERT,
      Map.of(),
      Map.of("id", 101, "amount", 55.25),
      "45",
      Instant.parse("2026-02-15T20:00:01Z"),
      Map.of("adapter", "cockroachdb"));

    assertEquals(101, event.afterJson().getInteger("id"));
    assertEquals(55.25, event.afterJson().getDouble("amount"));
  }
}
