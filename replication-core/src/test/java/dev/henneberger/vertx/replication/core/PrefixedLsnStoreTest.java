package dev.henneberger.vertx.replication.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class PrefixedLsnStoreTest {

  @Test
  void prefixesKeysForLoadAndSave() throws Exception {
    InMemoryLsnStore delegate = new InMemoryLsnStore();
    PrefixedLsnStore store = new PrefixedLsnStore(delegate, "prod:");

    store.save("orders", "0/16B4F50");

    assertEquals("0/16B4F50", delegate.load("prod:orders").orElse(null));
    assertEquals("0/16B4F50", store.load("orders").orElse(null));
  }
}
