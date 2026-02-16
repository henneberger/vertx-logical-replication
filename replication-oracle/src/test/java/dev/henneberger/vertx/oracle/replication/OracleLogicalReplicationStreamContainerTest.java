package dev.henneberger.vertx.oracle.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.henneberger.vertx.replication.core.RetryPolicy;
import dev.henneberger.vertx.replication.core.SubscriptionRegistration;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

class OracleLogicalReplicationStreamContainerTest {

  @Test
  void streamsInsertFromOracleCdcTable() throws Exception {
    Assumptions.assumeTrue(
      DockerClientFactory.instance().isDockerAvailable(),
      "Docker is required for Testcontainers integration tests");

    try (GenericContainer<?> oracle = new GenericContainer<>("gvenzl/oracle-xe:21-slim")
      .withExposedPorts(1521)
      .withEnv("ORACLE_PASSWORD", "oracle_pw")
      .withStartupTimeout(Duration.ofMinutes(10))) {

      try {
        oracle.start();
      } catch (Exception startupError) {
        Assumptions.assumeTrue(false, "Oracle container startup failed: " + startupError.getMessage());
        return;
      }

      String host = oracle.getHost();
      int port = oracle.getFirstMappedPort();
      String jdbcUrl = "jdbc:oracle:thin:@//" + host + ':' + port + "/XEPDB1";

      try {
        withRetry(jdbcUrl, conn -> {
          try (Statement statement = conn.createStatement()) {
            statement.execute("CREATE TABLE CDC_EVENTS ("
              + "POSITION NUMBER(19) PRIMARY KEY, "
              + "OPERATION VARCHAR2(32) NOT NULL, "
              + "BEFORE_JSON VARCHAR2(4000), "
              + "AFTER_JSON VARCHAR2(4000), "
              + "COMMIT_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");
          }
        });
      } catch (Exception setupError) {
        Assumptions.assumeTrue(false, "Oracle setup failed in container: " + setupError.getMessage());
        return;
      }

      Vertx vertx = Vertx.vertx();
      OracleLogicalReplicationStream stream = new OracleLogicalReplicationStream(
        vertx,
        new OracleReplicationOptions()
          .setHost(host)
          .setPort(port)
          .setDatabase("XEPDB1")
          .setUser("system")
          .setPassword("oracle_pw")
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

      CompletableFuture<OracleChangeEvent> received = new CompletableFuture<>();
      SubscriptionRegistration registration = stream.startAndSubscribe(
        OracleChangeFilter.all().operations(OracleChangeEvent.Operation.INSERT),
        event -> {
          received.complete(event);
          return Future.succeededFuture();
        },
        received::completeExceptionally
      );

      try {
        registration.started().toCompletionStage().toCompletableFuture().get(45, TimeUnit.SECONDS);

        withRetry(jdbcUrl, conn -> {
          try (Statement statement = conn.createStatement()) {
            statement.execute("INSERT INTO CDC_EVENTS(POSITION, OPERATION, AFTER_JSON) VALUES "
              + "(1, 'INSERT', '{\"id\":101,\"amount\":55.25}')");
          }
        });

        OracleChangeEvent event = received.get(45, TimeUnit.SECONDS);
        assertEquals(OracleChangeEvent.Operation.INSERT, event.getOperation());
        Map<String, Object> after = event.getAfter();
        assertEquals(101, ((Number) after.get("id")).intValue());
      } finally {
        registration.subscription().cancel();
        stream.close();
        vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
      }
    }
  }

  private interface SqlWork {
    void run(Connection connection) throws Exception;
  }

  private static void withRetry(String jdbcUrl, SqlWork work) throws Exception {
    SQLException last = null;
    for (int i = 0; i < 35; i++) {
      try (Connection conn = DriverManager.getConnection(jdbcUrl, "system", "oracle_pw")) {
        work.run(conn);
        return;
      } catch (SQLException e) {
        last = e;
        Thread.sleep(1000);
      }
    }
    throw last;
  }
}
