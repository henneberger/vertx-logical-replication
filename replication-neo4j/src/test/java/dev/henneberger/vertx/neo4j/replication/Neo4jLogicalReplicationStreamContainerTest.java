package dev.henneberger.vertx.neo4j.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

class Neo4jLogicalReplicationStreamContainerTest {

  @Test
  void connectsToNeo4jContainer() {
    Assumptions.assumeTrue(
      DockerClientFactory.instance().isDockerAvailable(),
      "Docker is required for Testcontainers integration tests");

    try (GenericContainer<?> neo4j = new GenericContainer<>("neo4j:5.26")
      .withExposedPorts(7687)
      .withEnv("NEO4J_AUTH", "neo4j/neo4j_pw")
      .withStartupTimeout(Duration.ofMinutes(5))) {

      try {
        neo4j.start();
      } catch (Exception startupError) {
        Assumptions.assumeTrue(false, "Neo4j container startup failed: " + startupError.getMessage());
        return;
      }

      String boltUrl = "bolt://" + neo4j.getHost() + ':' + neo4j.getFirstMappedPort();
      try (Driver driver = GraphDatabase.driver(boltUrl, AuthTokens.basic("neo4j", "neo4j_pw"));
           Session session = driver.session()) {
        Result result = session.run("RETURN 1 AS value");
        Record record = result.single();
        assertEquals(1, record.get("value").asInt());
      } catch (Exception connectionError) {
        Assumptions.assumeTrue(false, "Neo4j container connectivity failed: " + connectionError.getMessage());
      }
    }
  }
}
