package dev.henneberger.vertx.cockroachdb.replication;

import static dev.henneberger.vertx.replication.core.ReplicationStreamContractKit.assertClosePreventsStart;
import static dev.henneberger.vertx.replication.core.ReplicationStreamContractKit.assertPreflightFailureTransitionsToFailed;

import io.vertx.core.Vertx;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class CockroachDbReplicationStreamContractTest {

  @Test
  void contract_closePreventsStart() {
    Vertx vertx = Vertx.vertx();
    CockroachDbLogicalReplicationStream stream = new CockroachDbLogicalReplicationStream(vertx, options());
    try {
      assertClosePreventsStart(stream, Duration.ofSeconds(5));
    } finally {
      vertx.close();
    }
  }

  @Test
  void contract_preflightFailureTransitionsToFailed() {
    Vertx vertx = Vertx.vertx();
    CockroachDbLogicalReplicationStream stream = new CockroachDbLogicalReplicationStream(vertx, options());
    try {
      assertPreflightFailureTransitionsToFailed(stream, Duration.ofSeconds(10));
    } finally {
      stream.close();
      vertx.close();
    }
  }

  private static CockroachDbReplicationOptions options() {
    return new CockroachDbReplicationOptions()
      .setHost("localhost")
      .setPort(26257)
      .setDatabase("db")
      .setUser("user")
      .setSourceTable("cdc_table");
  }
}
