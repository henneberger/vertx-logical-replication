package dev.henneberger.vertx.mariadb.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
  void streamsInsertFromMariaDbCdcTable() throws Exception {
    Assumptions.assumeTrue(
      DockerClientFactory.instance().isDockerAvailable(),
      "Docker is required for Testcontainers integration tests");

    try (MariaDBContainer<?> mariadb = new MariaDBContainer<>("mariadb:11.4")
      .withDatabaseName("cdcdb")
      .withUsername("cdc_user")
      .withPassword("cdc_password")
      .withStartupTimeout(Duration.ofMinutes(3))) {

      mariadb.start();

      try (Connection conn = openConnection(mariadb);
           Statement statement = conn.createStatement()) {
        statement.execute("CREATE TABLE cdc_orders ("
          + "position BIGINT AUTO_INCREMENT PRIMARY KEY,"
          + "operation VARCHAR(16) NOT NULL,"
          + "before_json JSON NULL,"
          + "after_json JSON NULL,"
          + "commit_ts TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP"
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
          .setSourceTable("cdc_orders")
          .setPollIntervalMs(200)
          .setPreflightEnabled(true));

      CompletableFuture<MariaDbChangeEvent> received = new CompletableFuture<>();
      SubscriptionRegistration registration = stream.startAndSubscribe(
        MariaDbChangeFilter.sources("cdc_orders").operations(MariaDbChangeEvent.Operation.INSERT),
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
          statement.execute("INSERT INTO cdc_orders(operation, before_json, after_json) VALUES ("
            + "'INSERT', NULL, '{\"id\":101,\"amount\":55.25}')");
        }

        MariaDbChangeEvent event = received.get(30, TimeUnit.SECONDS);
        assertNotNull(event);
        assertEquals(MariaDbChangeEvent.Operation.INSERT, event.getOperation());

        Map<String, Object> after = event.getAfter();
        assertEquals(101, ((Number) after.get("id")).intValue());
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
