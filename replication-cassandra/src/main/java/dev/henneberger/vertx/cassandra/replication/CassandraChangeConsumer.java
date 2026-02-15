package dev.henneberger.vertx.cassandra.replication;

import dev.henneberger.vertx.replication.core.ChangeConsumer;

@FunctionalInterface
public interface CassandraChangeConsumer extends ChangeConsumer<CassandraChangeEvent> {
}
