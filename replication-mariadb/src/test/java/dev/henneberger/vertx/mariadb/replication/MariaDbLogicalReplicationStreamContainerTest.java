package dev.henneberger.vertx.mariadb.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.testcontainers.containers.MariaDBContainer;

class MariaDbLogicalReplicationStreamContainerTest {

  @Test
  void streamsInsertFromMariaDbBinlog() throws Exception {
    Assumptions.assumeTrue(
      DockerClientFactory.instance().isDockerAvailable(),
      "Docker is required for Testcontainers integration tests");

    try (MariaDBContainer<?> mariadb = new MariaDBContainer<>("mariadb:11.4")
      .withDatabaseName("cdcdb")
      .withUsername("root")
      .withPassword("cdc_password")
      .withCommand("mariadbd",
        "--log-bin=mysql-bin",
        "--binlog-format=ROW",
        "--server-id=12345")
      .withStartupTimeout(Duration.ofMinutes(3))) {

      mariadb.start();

      try (Connection conn = openConnection(mariadb);
           Statement statement = conn.createStatement()) {
        statement.execute("CREATE TABLE orders ("
          + "id BIGINT AUTO_INCREMENT PRIMARY KEY,"
          + "amount DECIMAL(10,2) NOT NULL,"
          + "note VARCHAR(255) NOT NULL"
          + ")");
      }

      Vertx vertx = Vertx.vertx();
      MariaDbLogicalReplicationStream stream = new MariaDbLogicalReplicationStream(
        vertx,
        new MariaDbReplicationOptions()
          .setHost(mariadb.getHost())
          .setPort(mariadb.getFirstMappedPort())
          .setDatabase(mariadb.getDatabaseName())
          .setUser(mariadb.getUsername())
          .setPassword(mariadb.getPassword())
          .setSourceTable("orders")
          .setServerId(12346L)
          .setPreflightEnabled(true));

      CompletableFuture<MariaDbChangeEvent> received = new CompletableFuture<>();
      SubscriptionRegistration registration = stream.startAndSubscribe(
        MariaDbChangeFilter.sources("orders").operations(MariaDbChangeEvent.Operation.INSERT),
        event -> {
          received.complete(event);
          return Future.succeededFuture();
        },
        received::completeExceptionally
      );

      try {
        registration.started().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);

        try (Connection conn = openConnection(mariadb);
             Statement statement = conn.createStatement()) {
          statement.execute("INSERT INTO orders(amount, note) VALUES (55.25, 'hello-binlog')");
        }

        MariaDbChangeEvent event = received.get(30, TimeUnit.SECONDS);
        assertNotNull(event);
        assertEquals(MariaDbChangeEvent.Operation.INSERT, event.getOperation());
        assertEquals("orders", event.getSource());
        assertTrue(event.getPosition() != null && !event.getPosition().isBlank());
        assertEquals("mariadb", event.getMetadata().get("adapter"));
        assertEquals("orders", event.getMetadata().get("table"));

        Map<String, Object> after = event.getAfter();
        assertNotNull(after.get("c0"));
        assertEquals(55.25d, ((Number) after.get("c1")).doubleValue(), 0.0001d);
      } finally {
        registration.subscription().cancel();
        stream.close();
        vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
      }
    }
  }

  private static Connection openConnection(MariaDBContainer<?> container) throws Exception {
    return DriverManager.getConnection(
      container.getJdbcUrl(),
      container.getUsername(),
      container.getPassword());
  }
}
