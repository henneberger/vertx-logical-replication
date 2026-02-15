package dev.henneberger.vertx.mysql.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import dev.henneberger.vertx.replication.core.InMemoryLsnStore;
import dev.henneberger.vertx.replication.core.RetryPolicy;
import dev.henneberger.vertx.replication.core.SubscriptionRegistration;
import io.vertx.core.Vertx;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.MySQLContainer;

class MySqlLogicalReplicationStreamContainerTest {

  @Test
  void streamsInsertFromMySqlBinlog() throws Exception {
    Assumptions.assumeTrue(
      DockerClientFactory.instance().isDockerAvailable(),
      "Docker is required for Testcontainers integration tests");

    try (MySQLContainer<?> mysql = new MySQLContainer<>("mysql:5.7")
      .withDatabaseName("cdcdb")
      .withUsername("cdc_user")
      .withPassword("cdc_password")
      .withEnv("MYSQL_ROOT_PASSWORD", "root_password")
      .withEnv("MYSQL_ROOT_HOST", "%")
      .withCommand(
        "--server-id=223344",
        "--log-bin=mysql-bin",
        "--binlog-format=ROW",
        "--binlog-row-image=FULL")
      .withStartupTimeout(Duration.ofMinutes(3))) {

      mysql.start();

      mysql.execInContainer(
        "mysql",
        "-uroot",
        "-proot_password",
        "-e",
        "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%'; FLUSH PRIVILEGES;");

      try (Connection conn = openConnection(mysql);
           Statement statement = conn.createStatement()) {
        statement.execute("CREATE TABLE orders (id INT PRIMARY KEY, amount DECIMAL(10,2) NOT NULL)");
      }

      String checkpoint;
      try {
        checkpoint = fetchCurrentCheckpoint(mysql);
      } catch (Exception privilegeIssue) {
        Assumptions.assumeTrue(false, "MySQL replication privileges unavailable in container: " + privilegeIssue.getMessage());
        return;
      }

      InMemoryLsnStore lsnStore = new InMemoryLsnStore();
      lsnStore.save("mysql:" + mysql.getDatabaseName(), checkpoint);

      Vertx vertx = Vertx.vertx();
      MySqlLogicalReplicationStream stream = new MySqlLogicalReplicationStream(
        vertx,
        new MySqlReplicationOptions()
          .setHost(mysql.getHost())
          .setPort(mysql.getFirstMappedPort())
          .setDatabase(mysql.getDatabaseName())
          .setUser(mysql.getUsername())
          .setPassword(mysql.getPassword())
          .setServerId(223355L)
          .setLsnStore(lsnStore)
          .setRetryPolicy(RetryPolicy.disabled())
          .setPreflightEnabled(true));

      CompletableFuture<MySqlChangeEvent> received = new CompletableFuture<>();
      SubscriptionRegistration registration = stream.startAndSubscribe(
        MySqlChangeFilter.tables(mysql.getDatabaseName() + ".orders")
          .operations(MySqlChangeEvent.Operation.INSERT),
        event -> {
          received.complete(event);
          return io.vertx.core.Future.succeededFuture();
        },
        received::completeExceptionally
      );

      try {
        registration.started().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);

        try (Connection conn = openConnection(mysql);
             Statement statement = conn.createStatement()) {
          statement.execute("INSERT INTO orders(id, amount) VALUES (101, 55.25)");
        }

        MySqlChangeEvent event = received.get(30, TimeUnit.SECONDS);
        assertNotNull(event);
        assertEquals(MySqlChangeEvent.Operation.INSERT, event.getOperation());

        Map<String, Object> after = event.getAfter();
        assertEquals(101, ((Number) after.get("c0")).intValue());
      } finally {
        registration.subscription().cancel();
        stream.close();
        vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
      }
    }
  }

  private static Connection openConnection(MySQLContainer<?> container) throws Exception {
    return DriverManager.getConnection(
      container.getJdbcUrl(),
      container.getUsername(),
      container.getPassword()
    );
  }

  private static String fetchCurrentCheckpoint(MySQLContainer<?> container) throws Exception {
    try (Connection conn = openConnection(container);
         Statement statement = conn.createStatement();
         ResultSet rs = statement.executeQuery("SHOW MASTER STATUS")) {
      if (!rs.next()) {
        throw new IllegalStateException("SHOW MASTER STATUS returned no rows");
      }
      return MySqlLogicalReplicationStream.formatCheckpoint(rs.getString("File"), rs.getLong("Position"));
    }
  }
}
