package dev.henneberger.vertx.mysql.replication;

import static dev.henneberger.vertx.replication.core.ReplicationStreamContractKit.assertClosePreventsStart;
import static dev.henneberger.vertx.replication.core.ReplicationStreamContractKit.assertPreflightFailureTransitionsToFailed;

import io.vertx.core.Vertx;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class MySqlReplicationStreamContractTest {

  @Test
  void contract_closePreventsStart() {
    Vertx vertx = Vertx.vertx();
    MySqlLogicalReplicationStream stream = new MySqlLogicalReplicationStream(vertx, options());
    try {
      assertClosePreventsStart(stream, Duration.ofSeconds(5));
    } finally {
      vertx.close();
    }
  }

  @Test
  void contract_preflightFailureTransitionsToFailed() {
    Vertx vertx = Vertx.vertx();
    MySqlLogicalReplicationStream stream = new MySqlLogicalReplicationStream(vertx, options());
    try {
      assertPreflightFailureTransitionsToFailed(stream, Duration.ofSeconds(10));
    } finally {
      stream.close();
      vertx.close();
    }
  }

  private static MySqlReplicationOptions options() {
    return new MySqlReplicationOptions()
      .setHost("localhost")
      .setPort(3306)
      .setDatabase("db")
      .setUser("user")
      .setServerId(1L)
      .setConnectTimeoutMs(1000L)
      .setPreflightEnabled(true);
  }
}
