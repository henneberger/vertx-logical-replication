package dev.henneberger.vertx.mongodb.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MongoDbLogicalReplicationStreamHelpersTest {

  @Test
  void filterMatchesBySourceAndOperation() {
    MongoDbChangeEvent event = new MongoDbChangeEvent(
      "orders",
      MongoDbChangeEvent.Operation.INSERT,
      Map.of(),
      Map.of("_id", 101),
      "10",
      Instant.parse("2026-02-15T20:00:00Z"),
      Map.of());

    MongoDbChangeFilter filter = MongoDbChangeFilter.sources("orders")
      .operations(MongoDbChangeEvent.Operation.INSERT);

    assertTrue(filter.test(event));
    assertFalse(MongoDbChangeFilter.sources("other").test(event));
    assertFalse(MongoDbChangeFilter.all().operations(MongoDbChangeEvent.Operation.DELETE).test(event));
  }

  @Test
  void eventAfterJsonContainsMappedFields() {
    MongoDbChangeEvent event = new MongoDbChangeEvent(
      "orders",
      MongoDbChangeEvent.Operation.UPDATE,
      Map.of("_id", 101),
      Map.of("_id", 101, "amount", 55.25),
      "11",
      Instant.parse("2026-02-15T20:00:01Z"),
      Map.of("adapter", "mongodb"));

    assertEquals(101, event.afterJson().getInteger("_id"));
    assertEquals(55.25, event.afterJson().getDouble("amount"));
  }
}
