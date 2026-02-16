package dev.henneberger.vertx.db2.replication;

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

class Db2LogicalReplicationStreamContainerTest {

  @Test
  void streamsInsertFromDb2CdcTable() throws Exception {
    Assumptions.assumeTrue(
      DockerClientFactory.instance().isDockerAvailable(),
      "Docker is required for Testcontainers integration tests");

    try (GenericContainer<?> db2 = new GenericContainer<>("icr.io/db2_community/db2:11.5.9.0")
      .withExposedPorts(50000)
      .withEnv("LICENSE", "accept")
      .withEnv("DBNAME", "testdb")
      .withEnv("DB2INST1_PASSWORD", "db2inst1_pw")
      .withEnv("ARCHIVE_LOGS", "false")
      .withEnv("AUTOCONFIG", "false")
      .withStartupTimeout(Duration.ofMinutes(12))) {

      try {
        db2.start();
      } catch (Exception startupError) {
        Assumptions.assumeTrue(false, "Db2 container startup failed: " + startupError.getMessage());
        return;
      }

      String jdbcUrl = "jdbc:db2://" + db2.getHost() + ':' + db2.getFirstMappedPort() + "/testdb";

      try (Connection conn = DriverManager.getConnection(jdbcUrl, "db2inst1", "db2inst1_pw");
           Statement statement = conn.createStatement()) {
        statement.execute("CREATE TABLE CDC_EVENTS ("
          + "POSITION BIGINT NOT NULL PRIMARY KEY, "
          + "OPERATION VARCHAR(32) NOT NULL, "
          + "BEFORE_JSON VARCHAR(4000), "
          + "AFTER_JSON VARCHAR(4000), "
          + "COMMIT_TS TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP)");
      } catch (Exception setupError) {
        Assumptions.assumeTrue(false, "Db2 setup failed in container: " + setupError.getMessage());
        return;
      }

      Vertx vertx = Vertx.vertx();
      Db2LogicalReplicationStream stream = new Db2LogicalReplicationStream(
        vertx,
        new Db2ReplicationOptions()
          .setHost(db2.getHost())
          .setPort(db2.getFirstMappedPort())
          .setDatabase("testdb")
          .setUser("db2inst1")
          .setPassword("db2inst1_pw")
          .setSourceTable("CDC_EVENTS")
          .setPositionColumn("POSITION")
          .setOperationColumn("OPERATION")
          .setBeforeColumn("BEFORE_JSON")
          .setAfterColumn("AFTER_JSON")
          .setCommitTimestampColumn("COMMIT_TS")
          .setPollIntervalMs(120)
          .setBatchSize(50)
          .setRetryPolicy(RetryPolicy.disabled())
          .setPreflightEnabled(true));

      CompletableFuture<Db2ChangeEvent> received = new CompletableFuture<>();
      SubscriptionRegistration registration = stream.startAndSubscribe(
        Db2ChangeFilter.all().operations(Db2ChangeEvent.Operation.INSERT),
        event -> {
          received.complete(event);
          return Future.succeededFuture();
        },
        received::completeExceptionally
      );

      try {
        registration.started().toCompletionStage().toCompletableFuture().get(45, TimeUnit.SECONDS);

        try (Connection conn = DriverManager.getConnection(jdbcUrl, "db2inst1", "db2inst1_pw");
             Statement statement = conn.createStatement()) {
          statement.execute("INSERT INTO CDC_EVENTS(POSITION, OPERATION, AFTER_JSON) VALUES "
            + "(1, 'INSERT', '{\"id\":101,\"amount\":55.25}')");
        }

        Db2ChangeEvent event = received.get(45, TimeUnit.SECONDS);
        assertEquals(Db2ChangeEvent.Operation.INSERT, event.getOperation());
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
