package dev.henneberger.vertx.replication.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class FileLsnStoreTest {

  @Test
  void persistsAndLoadsCheckpoint() throws Exception {
    Path file = Files.createTempFile("lsn-store", ".json");
    Files.deleteIfExists(file);

    FileLsnStore writer = new FileLsnStore(file);
    writer.save("stream-a", "0/16B4F50");

    FileLsnStore reader = new FileLsnStore(file);
    assertEquals("0/16B4F50", reader.load("stream-a").orElse(null));
  }

  @Test
  void returnsEmptyWhenMissing() throws Exception {
    Path file = Files.createTempFile("lsn-store", ".json");
    Files.deleteIfExists(file);

    FileLsnStore store = new FileLsnStore(file);
    assertFalse(store.load("missing").isPresent());

    store.save("stream-a", "1");
    assertTrue(store.load("stream-a").isPresent());
  }
}
