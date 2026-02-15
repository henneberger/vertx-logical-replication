package dev.henneberger.vertx.replication.core;

import java.util.Objects;
import java.util.Optional;

/**
 * Decorator that namespaces checkpoint keys with a fixed prefix.
 */
public final class PrefixedLsnStore implements LsnStore {

  private final LsnStore delegate;
  private final String prefix;

  public PrefixedLsnStore(LsnStore delegate, String prefix) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.prefix = Objects.requireNonNull(prefix, "prefix");
  }

  @Override
  public Optional<String> load(String streamName) throws Exception {
    Objects.requireNonNull(streamName, "streamName");
    return delegate.load(key(streamName));
  }

  @Override
  public void save(String streamName, String lsn) throws Exception {
    Objects.requireNonNull(streamName, "streamName");
    Objects.requireNonNull(lsn, "lsn");
    delegate.save(key(streamName), lsn);
  }

  private String key(String streamName) {
    return prefix + streamName;
  }
}
