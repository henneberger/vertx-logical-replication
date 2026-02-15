package dev.henneberger.vertx.sqlserver.replication;

import dev.henneberger.vertx.replication.core.ChangeConsumer;

@FunctionalInterface
public interface SqlServerChangeConsumer extends ChangeConsumer<SqlServerChangeEvent> {
}
