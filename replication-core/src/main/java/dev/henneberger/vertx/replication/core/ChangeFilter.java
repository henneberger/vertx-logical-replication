package dev.henneberger.vertx.replication.core;

@FunctionalInterface
public interface ChangeFilter<E> {
  boolean test(E event);
}
