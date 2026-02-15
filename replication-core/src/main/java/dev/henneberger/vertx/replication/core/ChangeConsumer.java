package dev.henneberger.vertx.replication.core;

import io.vertx.core.Future;

@FunctionalInterface
public interface ChangeConsumer<E> {
  Future<Void> handle(E event);
}
