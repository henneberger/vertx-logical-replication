package dev.henneberger.vertx.mongodb.replication;

import static dev.henneberger.vertx.replication.core.ReplicationStreamContractKit.assertClosePreventsStart;
import static dev.henneberger.vertx.replication.core.ReplicationStreamContractKit.assertPreflightFailureTransitionsToFailed;

import io.vertx.core.Vertx;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class MongoDbReplicationStreamContractTest {

  @Test
  void contract_closePreventsStart() {
    Vertx vertx = Vertx.vertx();
    MongoDbLogicalReplicationStream stream = new MongoDbLogicalReplicationStream(vertx, options());
    try {
      assertClosePreventsStart(stream, Duration.ofSeconds(5));
    } finally {
      vertx.close();
    }
  }

  @Test
  void contract_preflightFailureTransitionsToFailed() {
    Vertx vertx = Vertx.vertx();
    MongoDbLogicalReplicationStream stream = new MongoDbLogicalReplicationStream(vertx, options());
    try {
      assertPreflightFailureTransitionsToFailed(stream, Duration.ofSeconds(10));
    } finally {
      stream.close();
      vertx.close();
    }
  }

  private static MongoDbReplicationOptions options() {
    return new MongoDbReplicationOptions()
      .setConnectionString("mongodb://localhost:27017/?serverSelectionTimeoutMS=1000")
      .setDatabase("db")
      .setCollection("events")
      .setPreflightEnabled(true);
  }
}
