package dev.henneberger.vertx.mysql.replication;

import dev.henneberger.vertx.replication.core.ReplicationSubscription;

@FunctionalInterface
public interface MySqlChangeSubscription extends ReplicationSubscription {
  @Override
  void cancel();
}
