package dev.henneberger.vertx.cockroachdb.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import dev.henneberger.vertx.replication.core.RetryPolicy;
import dev.henneberger.vertx.replication.core.SubscriptionRegistration;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

class CockroachDbLogicalReplicationStreamContainerTest {

  @Test
  void streamsInsertFromCockroachDbCdcTable() throws Exception {
    Assumptions.assumeTrue(
      DockerClientFactory.instance().isDockerAvailable(),
      "Docker is required for Testcontainers integration tests");

    try (GenericContainer<?> cockroach = new GenericContainer<>("cockroachdb/cockroach:v24.3.3")
      .withExposedPorts(26257)
      .withCommand("start-single-node", "--insecure")
      .withStartupTimeout(Duration.ofMinutes(4))) {

      try {
        cockroach.start();
      } catch (Exception startupError) {
        Assumptions.assumeTrue(false, "CockroachDB container startup failed: " + startupError.getMessage());
        return;
      }

      String jdbcUrl = "jdbc:postgresql://" + cockroach.getHost() + ':' + cockroach.getFirstMappedPort()
        + "/defaultdb?sslmode=disable";

      try (Connection conn = DriverManager.getConnection(jdbcUrl, "root", "");
           Statement statement = conn.createStatement()) {
        statement.execute("CREATE TABLE IF NOT EXISTS cdc_events ("
          + "position BIGINT PRIMARY KEY, "
          + "operation STRING NOT NULL, "
          + "before_json STRING, "
          + "after_json STRING, "
          + "commit_ts TIMESTAMPTZ NOT NULL DEFAULT now())");
      }

      Vertx vertx = Vertx.vertx();
      CockroachDbLogicalReplicationStream stream = new CockroachDbLogicalReplicationStream(
        vertx,
        new CockroachDbReplicationOptions()
          .setHost(cockroach.getHost())
          .setPort(cockroach.getFirstMappedPort())
          .setDatabase("defaultdb")
          .setUser("root")
          .setPassword("")
          .setSourceTable("cdc_events")
          .setPollIntervalMs(100)
          .setBatchSize(50)
          .setRetryPolicy(RetryPolicy.disabled())
          .setPreflightEnabled(true));

      CompletableFuture<CockroachDbChangeEvent> received = new CompletableFuture<>();
      SubscriptionRegistration registration = stream.startAndSubscribe(
        CockroachDbChangeFilter.all().operations(CockroachDbChangeEvent.Operation.INSERT),
        event -> {
          received.complete(event);
          return Future.succeededFuture();
        },
        received::completeExceptionally
      );

      try {
        registration.started().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);

        try (Connection conn = DriverManager.getConnection(jdbcUrl, "root", "");
             Statement statement = conn.createStatement()) {
          statement.execute("INSERT INTO cdc_events(position, operation, after_json) VALUES "
            + "(1, 'INSERT', '{\"id\":101,\"amount\":55.25}')");
        }

        CockroachDbChangeEvent event = received.get(30, TimeUnit.SECONDS);
        assertEquals(CockroachDbChangeEvent.Operation.INSERT, event.getOperation());
        assertNotNull(event.getCommitTimestamp());

        Map<String, Object> after = event.getAfter();
        assertEquals(101, ((Number) after.get("id")).intValue());
      } finally {
        registration.subscription().cancel();
        stream.close();
        vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
      }
    }
  }
}
