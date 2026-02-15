package dev.henneberger.vertx.replication.core;

import java.util.Optional;

public final class NoopLsnStore implements LsnStore {
  @Override
  public Optional<String> load(String streamName) {
    return Optional.empty();
  }

  @Override
  public void save(String streamName, String lsn) {
    // no-op
  }
}
