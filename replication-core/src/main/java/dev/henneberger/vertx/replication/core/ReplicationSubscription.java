package dev.henneberger.vertx.replication.core;

@FunctionalInterface
public interface ReplicationSubscription {
  void cancel();
}
