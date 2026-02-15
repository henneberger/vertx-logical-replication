package dev.henneberger.vertx.cassandra.replication;

import static dev.henneberger.vertx.replication.core.ReplicationStreamContractKit.assertClosePreventsStart;
import static dev.henneberger.vertx.replication.core.ReplicationStreamContractKit.assertPreflightFailureTransitionsToFailed;

import io.vertx.core.Vertx;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class CassandraReplicationStreamContractTest {

  @Test
  void contract_closePreventsStart() {
    Vertx vertx = Vertx.vertx();
    CassandraLogicalReplicationStream stream = new CassandraLogicalReplicationStream(vertx, options());
    try {
      assertClosePreventsStart(stream, Duration.ofSeconds(5));
    } finally {
      vertx.close();
    }
  }

  @Test
  void contract_preflightFailureTransitionsToFailed() {
    Vertx vertx = Vertx.vertx();
    CassandraLogicalReplicationStream stream = new CassandraLogicalReplicationStream(vertx, options());
    try {
      assertPreflightFailureTransitionsToFailed(stream, Duration.ofSeconds(10));
    } finally {
      stream.close();
      vertx.close();
    }
  }

  private static CassandraReplicationOptions options() {
    return new CassandraReplicationOptions()
      .setHost("localhost")
      .setPort(9042)
      .setLocalDatacenter("dc1")
      .setKeyspace("ks")
      .setSourceTable("events")
      .setUser("user")
      .setPreflightEnabled(true);
  }
}
