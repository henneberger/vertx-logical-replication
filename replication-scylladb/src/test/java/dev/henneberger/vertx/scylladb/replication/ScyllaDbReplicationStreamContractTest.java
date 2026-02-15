package dev.henneberger.vertx.scylladb.replication;

import static dev.henneberger.vertx.replication.core.ReplicationStreamContractKit.assertClosePreventsStart;
import static dev.henneberger.vertx.replication.core.ReplicationStreamContractKit.assertPreflightFailureTransitionsToFailed;

import io.vertx.core.Vertx;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class ScyllaDbReplicationStreamContractTest {

  @Test
  void contract_closePreventsStart() {
    Vertx vertx = Vertx.vertx();
    ScyllaDbLogicalReplicationStream stream = new ScyllaDbLogicalReplicationStream(vertx, options());
    try {
      assertClosePreventsStart(stream, Duration.ofSeconds(5));
    } finally {
      vertx.close();
    }
  }

  @Test
  void contract_preflightFailureTransitionsToFailed() {
    Vertx vertx = Vertx.vertx();
    ScyllaDbLogicalReplicationStream stream = new ScyllaDbLogicalReplicationStream(vertx, options());
    try {
      assertPreflightFailureTransitionsToFailed(stream, Duration.ofSeconds(10));
    } finally {
      stream.close();
      vertx.close();
    }
  }

  private static ScyllaDbReplicationOptions options() {
    return new ScyllaDbReplicationOptions()
      .setHost("localhost")
      .setPort(9042)
      .setLocalDatacenter("dc1")
      .setKeyspace("ks")
      .setSourceTable("events")
      .setUser("user")
      .setPreflightEnabled(true);
  }
}
