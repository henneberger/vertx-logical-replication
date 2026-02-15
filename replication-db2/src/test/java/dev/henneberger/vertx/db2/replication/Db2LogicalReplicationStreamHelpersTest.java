package dev.henneberger.vertx.db2.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class Db2LogicalReplicationStreamHelpersTest {

  @Test
  void filterMatchesBySourceAndOperation() {
    Db2ChangeEvent event = new Db2ChangeEvent(
      "CDC_ORDERS",
      Db2ChangeEvent.Operation.INSERT,
      Map.of(),
      Map.of("id", 101),
      "10",
      Instant.parse("2026-02-15T20:00:00Z"),
      Map.of());

    Db2ChangeFilter filter = Db2ChangeFilter.sources("CDC_ORDERS")
      .operations(Db2ChangeEvent.Operation.INSERT);

    assertTrue(filter.test(event));
    assertFalse(Db2ChangeFilter.sources("OTHER").test(event));
    assertFalse(Db2ChangeFilter.all().operations(Db2ChangeEvent.Operation.DELETE).test(event));
  }

  @Test
  void eventAfterJsonContainsMappedFields() {
    Db2ChangeEvent event = new Db2ChangeEvent(
      "CDC_ORDERS",
      Db2ChangeEvent.Operation.UPDATE,
      Map.of("id", 101),
      Map.of("id", 101, "amount", 55.25),
      "11",
      Instant.parse("2026-02-15T20:00:01Z"),
      Map.of("adapter", "db2"));

    assertEquals(101, event.afterJson().getInteger("id"));
    assertEquals(55.25, event.afterJson().getDouble("amount"));
  }
}
