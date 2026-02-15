package dev.henneberger.vertx.neo4j.replication;

import dev.henneberger.vertx.replication.core.ChangeConsumer;

@FunctionalInterface
public interface Neo4jChangeConsumer extends ChangeConsumer<Neo4jChangeEvent> {
}
