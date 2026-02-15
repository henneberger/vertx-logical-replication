package dev.henneberger.vertx.replication.core;

import io.vertx.core.Future;
import java.util.Objects;

public final class SubscriptionRegistration {
  private final ReplicationSubscription subscription;
  private final Future<Void> started;

  public SubscriptionRegistration(ReplicationSubscription subscription, Future<Void> started) {
    this.subscription = Objects.requireNonNull(subscription, "subscription");
    this.started = Objects.requireNonNull(started, "started");
  }

  public ReplicationSubscription subscription() {
    return subscription;
  }

  public Future<Void> started() {
    return started;
  }
}
