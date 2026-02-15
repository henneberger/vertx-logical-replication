package dev.henneberger.vertx.mongodb.replication;

import dev.henneberger.vertx.replication.core.ChangeConsumer;

@FunctionalInterface
public interface MongoDbChangeConsumer extends ChangeConsumer<MongoDbChangeEvent> {
}
