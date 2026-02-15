package dev.henneberger.vertx.replication.core;

public interface ReplicationMetricsListener<E> {
  void onEvent(E event);
  void onParseFailure(String payload, Throwable error);
  void onStateChange(ReplicationStateChange stateChange);
  void onLsnCommitted(String streamName, String lsn);
}
