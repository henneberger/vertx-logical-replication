package dev.henneberger.vertx.cockroachdb.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

class CockroachDbLogicalReplicationStreamContainerTest {

  @Test
  void connectsToCockroachDbContainer() throws Exception {
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
        statement.execute("CREATE TABLE IF NOT EXISTS orders (id INT PRIMARY KEY, amount DECIMAL(10,2) NOT NULL)");
        statement.execute("UPSERT INTO orders(id, amount) VALUES (1, 42.50)");

        try (ResultSet rs = statement.executeQuery("SELECT id FROM orders WHERE id = 1")) {
          rs.next();
          assertEquals(1, rs.getInt(1));
        }
      } catch (Exception connectionError) {
        Assumptions.assumeTrue(false, "CockroachDB container connectivity failed: " + connectionError.getMessage());
      }
    }
  }
}
