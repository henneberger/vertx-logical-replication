package dev.henneberger.vertx.mysql.replication;

import dev.henneberger.vertx.replication.core.ChangeConsumer;

@FunctionalInterface
public interface MySqlChangeConsumer extends ChangeConsumer<MySqlChangeEvent> {
}
