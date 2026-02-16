package dev.henneberger.vertx.neo4j.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.henneberger.vertx.replication.core.RetryPolicy;
import dev.henneberger.vertx.replication.core.SubscriptionRegistration;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

class Neo4jLogicalReplicationStreamContainerTest {

  @Test
  void streamsInsertFromNeo4jEventQuery() throws Exception {
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

      Vertx vertx = Vertx.vertx();
      Neo4jLogicalReplicationStream stream = new Neo4jLogicalReplicationStream(
        vertx,
        new Neo4jReplicationOptions()
          .setUri(boltUrl)
          .setDatabase("neo4j")
          .setUser("neo4j")
          .setPassword("neo4j_pw")
          .setSourceName("orders")
          .setEventQuery(
            "MATCH (e:CdcEvent) WHERE e.position > $lastPosition "
              + "RETURN coalesce(e.source, $source) AS source, coalesce(e.operation, 'UPDATE') AS operation, "
              + "{} AS before, {id: e.id, amount: e.amount} AS after, "
              + "toString(e.position) AS position, e.commitTimestamp AS commitTimestamp "
              + "ORDER BY e.position LIMIT $limit")
          .setPollIntervalMs(120)
          .setBatchSize(50)
          .setRetryPolicy(RetryPolicy.disabled())
          .setPreflightEnabled(true));

      CompletableFuture<Neo4jChangeEvent> received = new CompletableFuture<>();
      SubscriptionRegistration registration = stream.startAndSubscribe(
        Neo4jChangeFilter.all().operations(Neo4jChangeEvent.Operation.INSERT),
        event -> {
          received.complete(event);
          return Future.succeededFuture();
        },
        received::completeExceptionally
      );

      try {
        registration.started().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);

        try (Driver driver = GraphDatabase.driver(boltUrl, AuthTokens.basic("neo4j", "neo4j_pw"));
             Session session = driver.session()) {
          session.run(
            "CREATE (e:CdcEvent {position:$position, source:$source, operation:$operation, id:$id, amount:$amount, commitTimestamp:$commitTimestamp})",
            Map.of(
              "position", 1L,
              "source", "orders",
              "operation", "INSERT",
              "id", 101L,
              "amount", 55.25d,
              "commitTimestamp", Instant.now().toString()
            )
          ).consume();
        }

        Neo4jChangeEvent event = received.get(35, TimeUnit.SECONDS);
        assertEquals(Neo4jChangeEvent.Operation.INSERT, event.getOperation());
        assertEquals("orders", event.getSource());
        assertEquals(101, ((Number) event.getAfter().get("id")).intValue());
      } finally {
        registration.subscription().cancel();
        stream.close();
        vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
      }
    }
  }
}
