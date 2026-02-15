package dev.henneberger.vertx.replication.core;

import io.vertx.core.Vertx;
import io.vertx.core.shareddata.LocalMap;
import java.util.Objects;
import java.util.Optional;

/**
 * Stores checkpoints in a Vert.x shared-data local map.
 */
public final class LocalMapLsnStore implements LsnStore {

  private final LocalMap<String, String> storage;

  public LocalMapLsnStore(Vertx vertx, String mapName) {
    Objects.requireNonNull(vertx, "vertx");
    this.storage = vertx.sharedData().getLocalMap(Objects.requireNonNull(mapName, "mapName"));
  }

  @Override
  public Optional<String> load(String streamName) {
    Objects.requireNonNull(streamName, "streamName");
    return Optional.ofNullable(storage.get(streamName));
  }

  @Override
  public void save(String streamName, String lsn) {
    Objects.requireNonNull(streamName, "streamName");
    Objects.requireNonNull(lsn, "lsn");
    storage.put(streamName, lsn);
  }
}
