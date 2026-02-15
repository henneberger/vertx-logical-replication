package dev.henneberger.vertx.replication.core;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryLsnStore implements LsnStore {
  private final Map<String, String> storage = new ConcurrentHashMap<>();

  @Override
  public Optional<String> load(String streamName) {
    return Optional.ofNullable(storage.get(streamName));
  }

  @Override
  public void save(String streamName, String lsn) {
    storage.put(streamName, lsn);
  }
}
