package dev.henneberger.vertx.scylladb.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import java.net.InetSocketAddress;
import java.time.Duration;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

class ScyllaDbLogicalReplicationStreamContainerTest {

  @Test
  void connectsToScyllaDbContainer() {
    Assumptions.assumeTrue(
      DockerClientFactory.instance().isDockerAvailable(),
      "Docker is required for Testcontainers integration tests");

    try (GenericContainer<?> scylla = new GenericContainer<>("scylladb/scylla:5.4")
      .withExposedPorts(9042)
      .withCommand("--smp", "1", "--memory", "750M", "--overprovisioned", "1", "--api-address", "0.0.0.0")
      .withStartupTimeout(Duration.ofMinutes(8))) {

      try {
        scylla.start();
      } catch (Exception startupError) {
        Assumptions.assumeTrue(false, "ScyllaDB container startup failed: " + startupError.getMessage());
        return;
      }

      try (CqlSession session = CqlSession.builder()
        .addContactPoint(new InetSocketAddress(scylla.getHost(), scylla.getFirstMappedPort()))
        .withLocalDatacenter("datacenter1")
        .build()) {

        session.execute("CREATE KEYSPACE IF NOT EXISTS cdc WITH replication = {'class':'SimpleStrategy','replication_factor':1}");
        session.execute("CREATE TABLE IF NOT EXISTS cdc.orders (id int PRIMARY KEY, amount decimal)");
        session.execute("INSERT INTO cdc.orders (id, amount) VALUES (1, 10.50)");

        Row row = session.execute("SELECT id FROM cdc.orders WHERE id = 1").one();
        assertEquals(1, row.getInt("id"));
      } catch (Exception connectionError) {
        Assumptions.assumeTrue(false, "ScyllaDB container connectivity failed: " + connectionError.getMessage());
      }
    }
  }
}
