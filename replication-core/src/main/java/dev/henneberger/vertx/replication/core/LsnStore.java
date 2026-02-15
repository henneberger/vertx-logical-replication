package dev.henneberger.vertx.replication.core;

import java.util.Optional;

public interface LsnStore {
  Optional<String> load(String streamName) throws Exception;
  void save(String streamName, String lsn) throws Exception;
}
