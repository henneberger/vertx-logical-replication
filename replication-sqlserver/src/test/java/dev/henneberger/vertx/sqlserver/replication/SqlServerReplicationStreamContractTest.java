package dev.henneberger.vertx.sqlserver.replication;

import static dev.henneberger.vertx.replication.core.ReplicationStreamContractKit.assertClosePreventsStart;
import static dev.henneberger.vertx.replication.core.ReplicationStreamContractKit.assertPreflightFailureTransitionsToFailed;

import io.vertx.core.Vertx;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class SqlServerReplicationStreamContractTest {

  @Test
  void contract_closePreventsStart() {
    Vertx vertx = Vertx.vertx();
    SqlServerLogicalReplicationStream stream = new SqlServerLogicalReplicationStream(vertx, options());
    try {
      assertClosePreventsStart(stream, Duration.ofSeconds(5));
    } finally {
      vertx.close();
    }
  }

  @Test
  void contract_preflightFailureTransitionsToFailed() {
    Vertx vertx = Vertx.vertx();
    SqlServerLogicalReplicationStream stream = new SqlServerLogicalReplicationStream(vertx, options());
    try {
      assertPreflightFailureTransitionsToFailed(stream, Duration.ofSeconds(10));
    } finally {
      stream.close();
      vertx.close();
    }
  }

  private static SqlServerReplicationOptions options() {
    return new SqlServerReplicationOptions()
      .setHost("localhost")
      .setPort(1433)
      .setDatabase("db")
      .setUser("user")
      .setCaptureInstance("ci")
      .setPreflightEnabled(true);
  }
}
