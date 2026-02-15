package dev.henneberger.vertx.neo4j.replication;

import static dev.henneberger.vertx.replication.core.ReplicationStreamContractKit.assertClosePreventsStart;
import static dev.henneberger.vertx.replication.core.ReplicationStreamContractKit.assertPreflightFailureTransitionsToFailed;

import io.vertx.core.Vertx;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class Neo4jReplicationStreamContractTest {

  @Test
  void contract_closePreventsStart() {
    Vertx vertx = Vertx.vertx();
    Neo4jLogicalReplicationStream stream = new Neo4jLogicalReplicationStream(vertx, options());
    try {
      assertClosePreventsStart(stream, Duration.ofSeconds(5));
    } finally {
      vertx.close();
    }
  }

  @Test
  void contract_preflightFailureTransitionsToFailed() {
    Vertx vertx = Vertx.vertx();
    Neo4jLogicalReplicationStream stream = new Neo4jLogicalReplicationStream(vertx, options());
    try {
      assertPreflightFailureTransitionsToFailed(stream, Duration.ofSeconds(10));
    } finally {
      stream.close();
      vertx.close();
    }
  }

  private static Neo4jReplicationOptions options() {
    return new Neo4jReplicationOptions()
      .setUri("neo4j://localhost:7687")
      .setDatabase("neo4j")
      .setUser("neo4j")
      .setSourceName("neo_events")
      .setEventQuery("RETURN 'neo_events' AS source, 'UPDATE' AS operation, {} AS before, {} AS after, '1' AS position, datetime() AS commitTimestamp LIMIT $limit")
      .setPreflightEnabled(true);
  }
}
