package dev.henneberger.vertx.sqlserver.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.henneberger.vertx.replication.core.SubscriptionRegistration;
import io.vertx.core.Vertx;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.utility.DockerImageName;

class SqlServerLogicalReplicationStreamContainerTest {

  @Test
  void streamsInsertFromSqlServerCdc() throws Exception {
    Assumptions.assumeTrue(
      DockerClientFactory.instance().isDockerAvailable(),
      "Docker is required for Testcontainers integration tests");

    MSSQLServerContainer<?> sqlserver = new MSSQLServerContainer<>(
      DockerImageName.parse("mcr.microsoft.com/mssql/server:2022-CU14-ubuntu-22.04"))
      .acceptLicense()
      .withEnv("MSSQL_AGENT_ENABLED", "true")
      .withStartupTimeout(Duration.ofMinutes(5));

    try {
      sqlserver.start();

      String dbName = "cdcdb";
      String captureInstance = "dbo_orders";

      try (Connection conn = openConnection(sqlserver, "master");
           Statement statement = conn.createStatement()) {
        statement.execute("IF DB_ID('" + dbName + "') IS NULL CREATE DATABASE " + dbName);
      }

      try (Connection conn = openConnection(sqlserver, dbName);
           Statement statement = conn.createStatement()) {
        statement.execute("EXEC sys.sp_cdc_enable_db");
        statement.execute("IF OBJECT_ID('dbo.orders', 'U') IS NULL CREATE TABLE dbo.orders (id INT PRIMARY KEY, amount DECIMAL(10,2) NOT NULL)");
        statement.execute("EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='orders', @role_name=NULL, @supports_net_changes=0");
        statement.execute("EXEC sys.sp_cdc_start_job @job_type='capture'");
      } catch (Exception setupError) {
        Assumptions.assumeTrue(false, "SQL Server CDC setup failed in container: " + setupError.getMessage());
      }

      Vertx vertx = Vertx.vertx();
      SqlServerLogicalReplicationStream stream = new SqlServerLogicalReplicationStream(
        vertx,
        new SqlServerReplicationOptions()
          .setHost(sqlserver.getHost())
          .setPort(sqlserver.getFirstMappedPort())
          .setDatabase(dbName)
          .setUser(sqlserver.getUsername())
          .setPassword(sqlserver.getPassword())
          .setCaptureInstance(captureInstance)
          .setPollIntervalMs(250)
          .setPreflightEnabled(true));

      CompletableFuture<SqlServerChangeEvent> received = new CompletableFuture<>();
      SubscriptionRegistration registration = stream.startAndSubscribe(
        SqlServerChangeFilter.instances(captureInstance),
        event -> {
          if (event.getOperation() == SqlServerChangeEvent.Operation.INSERT) {
            received.complete(event);
          }
          return io.vertx.core.Future.succeededFuture();
        },
        received::completeExceptionally
      );

      try {
        registration.started().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);

        try (Connection conn = openConnection(sqlserver, dbName);
             Statement statement = conn.createStatement()) {
          statement.execute("INSERT INTO dbo.orders(id, amount) VALUES (101, 55.25)");
        }

        SqlServerChangeEvent event = received.get(60, TimeUnit.SECONDS);
        assertNotNull(event);
        assertEquals(SqlServerChangeEvent.Operation.INSERT, event.getOperation());

        Map<String, Object> after = event.getAfter();
        Object id = findIgnoringCase(after, "id");
        assertEquals(101, ((Number) id).intValue());
      } finally {
        registration.subscription().cancel();
        stream.close();
        vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
      }
    } finally {
      sqlserver.stop();
    }
  }

  @Test
  void consumesBurstLargerThanBatchWithoutLossOrDuplication() throws Exception {
    Assumptions.assumeTrue(
      DockerClientFactory.instance().isDockerAvailable(),
      "Docker is required for Testcontainers integration tests");

    MSSQLServerContainer<?> sqlserver = new MSSQLServerContainer<>(
      DockerImageName.parse("mcr.microsoft.com/mssql/server:2022-CU14-ubuntu-22.04"))
      .acceptLicense()
      .withEnv("MSSQL_AGENT_ENABLED", "true")
      .withStartupTimeout(Duration.ofMinutes(5));

    try {
      sqlserver.start();

      String dbName = "cdcdb2";
      String captureInstance = "dbo_orders";
      int eventCount = 20;

      try (Connection conn = openConnection(sqlserver, "master");
           Statement statement = conn.createStatement()) {
        statement.execute("IF DB_ID('" + dbName + "') IS NULL CREATE DATABASE " + dbName);
      }

      try (Connection conn = openConnection(sqlserver, dbName);
           Statement statement = conn.createStatement()) {
        statement.execute("EXEC sys.sp_cdc_enable_db");
        statement.execute("IF OBJECT_ID('dbo.orders', 'U') IS NULL CREATE TABLE dbo.orders (id INT PRIMARY KEY, amount DECIMAL(10,2) NOT NULL)");
        statement.execute("EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='orders', @role_name=NULL, @supports_net_changes=0");
        statement.execute("EXEC sys.sp_cdc_start_job @job_type='capture'");
      } catch (Exception setupError) {
        Assumptions.assumeTrue(false, "SQL Server CDC setup failed in container: " + setupError.getMessage());
      }

      Vertx vertx = Vertx.vertx();
      SqlServerLogicalReplicationStream stream = new SqlServerLogicalReplicationStream(
        vertx,
        new SqlServerReplicationOptions()
          .setHost(sqlserver.getHost())
          .setPort(sqlserver.getFirstMappedPort())
          .setDatabase(dbName)
          .setUser(sqlserver.getUsername())
          .setPassword(sqlserver.getPassword())
          .setCaptureInstance(captureInstance)
          .setMaxBatchSize(3)
          .setPollIntervalMs(150)
          .setPreflightEnabled(true));

      Set<Integer> ids = ConcurrentHashMap.newKeySet();
      CompletableFuture<Void> allReceived = new CompletableFuture<>();
      SubscriptionRegistration registration = stream.startAndSubscribe(
        SqlServerChangeFilter.instances(captureInstance),
        event -> {
          if (event.getOperation() == SqlServerChangeEvent.Operation.INSERT) {
            Object idObj = findIgnoringCase(event.getAfter(), "id");
            if (idObj != null) {
              ids.add(((Number) idObj).intValue());
              if (ids.size() >= eventCount) {
                allReceived.complete(null);
              }
            }
          }
          return io.vertx.core.Future.succeededFuture();
        },
        allReceived::completeExceptionally
      );

      try {
        registration.started().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);

        try (Connection conn = openConnection(sqlserver, dbName);
             Statement statement = conn.createStatement()) {
          for (int i = 1; i <= eventCount; i++) {
            statement.execute("INSERT INTO dbo.orders(id, amount) VALUES (" + i + ", 10.00)");
          }
        }

        allReceived.get(90, TimeUnit.SECONDS);
        assertEquals(eventCount, ids.size());
        for (int i = 1; i <= eventCount; i++) {
          assertTrue(ids.contains(i), "Missing id=" + i);
        }
      } finally {
        registration.subscription().cancel();
        stream.close();
        vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
      }
    } finally {
      sqlserver.stop();
    }
  }

  private static Connection openConnection(MSSQLServerContainer<?> container, String database) throws Exception {
    String jdbc = "jdbc:sqlserver://" + container.getHost() + ':' + container.getFirstMappedPort()
      + ";databaseName=" + database + ";encrypt=false;trustServerCertificate=true";
    return DriverManager.getConnection(jdbc, container.getUsername(), container.getPassword());
  }

  private static Object findIgnoringCase(Map<String, Object> values, String key) {
    for (Map.Entry<String, Object> entry : values.entrySet()) {
      if (entry.getKey().toLowerCase(Locale.ROOT).equals(key.toLowerCase(Locale.ROOT))) {
        return entry.getValue();
      }
    }
    return null;
  }
}
