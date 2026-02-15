package dev.henneberger.vertx.oracle.replication;

import dev.henneberger.vertx.replication.core.ChangeConsumer;

@FunctionalInterface
public interface OracleChangeConsumer extends ChangeConsumer<OracleChangeEvent> {
}
