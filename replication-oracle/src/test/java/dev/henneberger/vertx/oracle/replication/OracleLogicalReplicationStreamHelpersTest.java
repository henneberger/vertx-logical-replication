package dev.henneberger.vertx.oracle.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OracleLogicalReplicationStreamHelpersTest {

  @Test
  void filterMatchesBySourceAndOperation() {
    OracleChangeEvent event = new OracleChangeEvent(
      "CDC_ORDERS",
      OracleChangeEvent.Operation.INSERT,
      Map.of(),
      Map.of("id", 101),
      "10",
      Instant.parse("2026-02-15T20:00:00Z"),
      Map.of());

    OracleChangeFilter filter = OracleChangeFilter.sources("CDC_ORDERS")
      .operations(OracleChangeEvent.Operation.INSERT);

    assertTrue(filter.test(event));
    assertFalse(OracleChangeFilter.sources("OTHER").test(event));
    assertFalse(OracleChangeFilter.all().operations(OracleChangeEvent.Operation.DELETE).test(event));
  }

  @Test
  void eventAfterJsonContainsMappedFields() {
    OracleChangeEvent event = new OracleChangeEvent(
      "CDC_ORDERS",
      OracleChangeEvent.Operation.UPDATE,
      Map.of("id", 101),
      Map.of("id", 101, "amount", 55.25),
      "11",
      Instant.parse("2026-02-15T20:00:01Z"),
      Map.of("adapter", "oracle"));

    assertEquals(101, event.afterJson().getInteger("id"));
    assertEquals(55.25, event.afterJson().getDouble("amount"));
  }
}
