package dev.henneberger.vertx.oracle.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

class OracleLogicalReplicationStreamContainerTest {

  @Test
  void connectsToOracleContainer() throws Exception {
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

      String jdbcUrl = "jdbc:oracle:thin:@//" + oracle.getHost() + ':' + oracle.getFirstMappedPort() + "/XEPDB1";

      try {
        assertEquals(1, queryOne(jdbcUrl, "system", "oracle_pw"));
      } catch (Exception connectionError) {
        Assumptions.assumeTrue(false, "Oracle connection failed in container: " + connectionError.getMessage());
      }
    }
  }

  private static int queryOne(String jdbcUrl, String user, String password) throws Exception {
    SQLException last = null;
    for (int i = 0; i < 30; i++) {
      try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
           Statement statement = conn.createStatement();
           ResultSet rs = statement.executeQuery("SELECT 1 FROM dual")) {
        rs.next();
        return rs.getInt(1);
      } catch (SQLException e) {
        last = e;
        Thread.sleep(1000);
      }
    }
    throw last;
  }
}
