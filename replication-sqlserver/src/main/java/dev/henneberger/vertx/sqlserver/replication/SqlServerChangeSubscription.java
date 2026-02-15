package dev.henneberger.vertx.sqlserver.replication;

import dev.henneberger.vertx.replication.core.ReplicationSubscription;

@FunctionalInterface
public interface SqlServerChangeSubscription extends ReplicationSubscription {
  @Override
  void cancel();
}
