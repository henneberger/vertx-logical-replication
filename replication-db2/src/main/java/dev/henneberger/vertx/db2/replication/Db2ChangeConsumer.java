package dev.henneberger.vertx.db2.replication;

import dev.henneberger.vertx.replication.core.ChangeConsumer;

@FunctionalInterface
public interface Db2ChangeConsumer extends ChangeConsumer<Db2ChangeEvent> {
}
