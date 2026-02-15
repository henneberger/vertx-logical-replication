package dev.henneberger.vertx.replication.core;

import io.vertx.core.Future;
import io.vertx.core.Handler;

public interface ReplicationStream<E> extends AutoCloseable {
  Future<Void> start();
  Future<PreflightReport> preflight();
  default AdapterMode adapterMode() {
    return AdapterMode.POLLING;
  }
  ReplicationStreamState state();
  ReplicationSubscription onStateChange(Handler<ReplicationStateChange> handler);
  ReplicationSubscription addMetricsListener(ReplicationMetricsListener<E> listener);
  ReplicationSubscription subscribe(ChangeFilter<E> filter, ChangeConsumer<E> eventConsumer, Handler<Throwable> errorHandler);
  SubscriptionRegistration startAndSubscribe(ChangeFilter<E> filter, ChangeConsumer<E> eventConsumer, Handler<Throwable> errorHandler);
}
