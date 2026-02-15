package dev.henneberger.vertx.cockroachdb.replication;

import dev.henneberger.vertx.replication.core.ChangeConsumer;

@FunctionalInterface
public interface CockroachDbChangeConsumer extends ChangeConsumer<CockroachDbChangeEvent> {
}
