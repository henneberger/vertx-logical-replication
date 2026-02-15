package dev.henneberger.vertx.scylladb.replication;

import dev.henneberger.vertx.replication.core.ChangeConsumer;

@FunctionalInterface
public interface ScyllaDbChangeConsumer extends ChangeConsumer<ScyllaDbChangeEvent> {
}
