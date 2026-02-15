package dev.henneberger.vertx.mariadb.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MariaDbLogicalReplicationStreamHelpersTest {

  @Test
  void filterMatchesBySourceAndOperation() {
    MariaDbChangeEvent event = new MariaDbChangeEvent(
      "cdc_orders",
      MariaDbChangeEvent.Operation.INSERT,
      Map.of(),
      Map.of("id", 101),
      "12",
      Instant.parse("2026-02-15T20:00:00Z"),
      Map.of());

    MariaDbChangeFilter filter = MariaDbChangeFilter.sources("cdc_orders")
      .operations(MariaDbChangeEvent.Operation.INSERT);

    assertTrue(filter.test(event));
    assertFalse(MariaDbChangeFilter.sources("other_table").test(event));
    assertFalse(MariaDbChangeFilter.all().operations(MariaDbChangeEvent.Operation.DELETE).test(event));
  }

  @Test
  void eventAfterJsonContainsMappedFields() {
    MariaDbChangeEvent event = new MariaDbChangeEvent(
      "cdc_orders",
      MariaDbChangeEvent.Operation.UPDATE,
      Map.of("id", 101),
      Map.of("id", 101, "amount", 55.25),
      "13",
      Instant.parse("2026-02-15T20:00:01Z"),
      Map.of("adapter", "mariadb"));

    assertEquals(101, event.afterJson().getInteger("id"));
    assertEquals(55.25, event.afterJson().getDouble("amount"));
  }
}
