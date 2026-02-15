package dev.henneberger.vertx.mariadb.replication;

import dev.henneberger.vertx.replication.core.ChangeConsumer;

@FunctionalInterface
public interface MariaDbChangeConsumer extends ChangeConsumer<MariaDbChangeEvent> {
}
