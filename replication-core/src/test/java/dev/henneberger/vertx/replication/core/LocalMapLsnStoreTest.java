package dev.henneberger.vertx.replication.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.vertx.core.Vertx;
import org.junit.jupiter.api.Test;

class LocalMapLsnStoreTest {

  @Test
  void storesValuesInSharedDataLocalMap() {
    Vertx vertx = Vertx.vertx();
    try {
      LocalMapLsnStore store = new LocalMapLsnStore(vertx, "test-lsn-map");
      store.save("stream-a", "123");

      assertEquals("123", store.load("stream-a").orElse(null));
      assertFalse(store.load("stream-b").isPresent());
    } finally {
      vertx.close();
    }
  }
}
