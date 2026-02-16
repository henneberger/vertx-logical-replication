package dev.henneberger.vertx.cockroachdb.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.henneberger.vertx.replication.core.RetryPolicy;
import dev.henneberger.vertx.replication.core.SubscriptionRegistration;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

class CockroachDbLogicalReplicationStreamContainerTest {

  @Test
  void streamsInsertUpdateDeleteFromCockroachDbChangefeed() throws Exception {
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
        statement.execute("SET CLUSTER SETTING kv.rangefeed.enabled = true");
        statement.execute("CREATE TABLE IF NOT EXISTS orders ("
          + "id INT8 PRIMARY KEY, "
          + "amount DECIMAL(10,2) NOT NULL, "
          + "status STRING NOT NULL)");
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
          .setSourceTable("orders")
          .setCliCommand(java.util.List.of(
            "docker", "exec", "-i", cockroach.getContainerId(),
            "cockroach", "sql", "--insecure", "--format=csv", "--database=defaultdb", "-e"))
          .setRetryPolicy(RetryPolicy.disabled())
          .setPreflightEnabled(true));

      BlockingQueue<CockroachDbChangeEvent> events = new LinkedBlockingQueue<>();
      CompletableFuture<Throwable> errors = new CompletableFuture<>();
      SubscriptionRegistration registration = stream.startAndSubscribe(
        CockroachDbChangeFilter.all(),
        event -> {
          events.offer(event);
          return Future.succeededFuture();
        },
        errors::complete
      );

      try {
        registration.started().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);

        try (Connection conn = DriverManager.getConnection(jdbcUrl, "root", "");
             Statement statement = conn.createStatement()) {
          statement.execute("INSERT INTO orders(id, amount, status) VALUES (1, 55.25, 'NEW')");
          statement.execute("UPDATE orders SET status='PAID' WHERE id=1");
          statement.execute("DELETE FROM orders WHERE id=1");
        }

        CockroachDbChangeEvent insert = poll(events, "insert");
        CockroachDbChangeEvent update = poll(events, "update");
        CockroachDbChangeEvent delete = poll(events, "delete");

        assertEquals(CockroachDbChangeEvent.Operation.INSERT, insert.getOperation());
        assertTrue(insert.getSource().contains("orders"));
        assertNotNull(insert.getCommitTimestamp());
        assertTrue(insert.getPosition() != null && !insert.getPosition().isBlank());
        assertEquals(1L, ((Number) insert.getAfter().get("id")).longValue());
        assertEquals("NEW", insert.getAfter().get("status"));
        assertTrue(insert.getBefore().isEmpty());

        assertEquals(CockroachDbChangeEvent.Operation.UPDATE, update.getOperation());
        assertEquals("PAID", update.getAfter().get("status"));
        assertFalse(update.getBefore().isEmpty());
        assertEquals("NEW", update.getBefore().get("status"));

        assertEquals(CockroachDbChangeEvent.Operation.DELETE, delete.getOperation());
        assertTrue(delete.getAfter().isEmpty());
        assertFalse(delete.getBefore().isEmpty());
        assertEquals(1L, ((Number) delete.getBefore().get("id")).longValue());
        assertFalse(errors.isDone(), "Did not expect stream errors");
      } finally {
        registration.subscription().cancel();
        stream.close();
        vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
      }
    }
  }

  private static CockroachDbChangeEvent poll(BlockingQueue<CockroachDbChangeEvent> events, String label) throws Exception {
    CockroachDbChangeEvent event = events.poll(30, TimeUnit.SECONDS);
    if (event == null) {
      throw new IllegalStateException("Timed out waiting for " + label + " event");
    }
    return event;
  }
}
