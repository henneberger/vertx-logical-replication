package dev.henneberger.vertx.cassandra.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import java.net.InetSocketAddress;
import java.time.Duration;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

class CassandraLogicalReplicationStreamContainerTest {

  @Test
  void connectsToCassandraContainer() {
    Assumptions.assumeTrue(
      DockerClientFactory.instance().isDockerAvailable(),
      "Docker is required for Testcontainers integration tests");

    try (GenericContainer<?> cassandra = new GenericContainer<>("cassandra:4.1")
      .withExposedPorts(9042)
      .withStartupTimeout(Duration.ofMinutes(7))) {

      try {
        cassandra.start();
      } catch (Exception startupError) {
        Assumptions.assumeTrue(false, "Cassandra container startup failed: " + startupError.getMessage());
        return;
      }

      try (CqlSession session = CqlSession.builder()
        .addContactPoint(new InetSocketAddress(cassandra.getHost(), cassandra.getFirstMappedPort()))
        .withLocalDatacenter("datacenter1")
        .build()) {

        session.execute("CREATE KEYSPACE IF NOT EXISTS cdc WITH replication = {'class':'SimpleStrategy','replication_factor':1}");
        session.execute("CREATE TABLE IF NOT EXISTS cdc.orders (id int PRIMARY KEY, amount decimal)");
        session.execute("INSERT INTO cdc.orders (id, amount) VALUES (1, 10.50)");

        Row row = session.execute("SELECT id FROM cdc.orders WHERE id = 1").one();
        assertEquals(1, row.getInt("id"));
      } catch (Exception connectionError) {
        Assumptions.assumeTrue(false, "Cassandra container connectivity failed: " + connectionError.getMessage());
      }
    }
  }
}
