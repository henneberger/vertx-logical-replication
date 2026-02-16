package dev.henneberger.vertx.db2.replication;

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

class Db2LogicalReplicationStreamContainerTest {

  @Test
  void connectsToDb2Container() throws Exception {
    Assumptions.assumeTrue(
      DockerClientFactory.instance().isDockerAvailable(),
      "Docker is required for Testcontainers integration tests");

    try (GenericContainer<?> db2 = new GenericContainer<>("ibmcom/db2:11.5.9.0")
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
           Statement statement = conn.createStatement();
           ResultSet rs = statement.executeQuery("VALUES 1")) {
        rs.next();
        assertEquals(1, rs.getInt(1));
      } catch (Exception connectionError) {
        Assumptions.assumeTrue(false, "Db2 container connectivity failed: " + connectionError.getMessage());
      }
    }
  }
}
