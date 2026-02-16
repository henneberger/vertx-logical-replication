package dev.henneberger.vertx.cassandra.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import dev.henneberger.vertx.replication.core.RetryPolicy;
import dev.henneberger.vertx.replication.core.SubscriptionRegistration;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

class CassandraLogicalReplicationStreamContainerTest {

  @Test
  void streamsInsertFromCassandraCdcTable() throws Exception {
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
        session.execute("CREATE TABLE IF NOT EXISTS cdc.cdc_events (position bigint PRIMARY KEY, operation text, id int, amount decimal)");
      }

      Vertx vertx = Vertx.vertx();
      CassandraLogicalReplicationStream stream = new CassandraLogicalReplicationStream(
        vertx,
        new CassandraReplicationOptions()
          .setHost(cassandra.getHost())
          .setPort(cassandra.getFirstMappedPort())
          .setLocalDatacenter("datacenter1")
          .setKeyspace("cdc")
          .setSourceTable("cdc_events")
          .setPollIntervalMs(120)
          .setBatchSize(50)
          .setRetryPolicy(RetryPolicy.disabled())
          .setPreflightEnabled(true));

      CompletableFuture<CassandraChangeEvent> received = new CompletableFuture<>();
      SubscriptionRegistration registration = stream.startAndSubscribe(
        CassandraChangeFilter.all().operations(CassandraChangeEvent.Operation.INSERT),
        event -> {
          received.complete(event);
          return Future.succeededFuture();
        },
        received::completeExceptionally
      );

      try {
        registration.started().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);

        try (CqlSession session = CqlSession.builder()
          .addContactPoint(new InetSocketAddress(cassandra.getHost(), cassandra.getFirstMappedPort()))
          .withLocalDatacenter("datacenter1")
          .withKeyspace("cdc")
          .build()) {
          session.execute(SimpleStatement.newInstance(
            "INSERT INTO cdc_events(position, operation, id, amount) VALUES (?, ?, ?, ?)",
            1L, "INSERT", 101, java.math.BigDecimal.valueOf(55.25)));
        }

        CassandraChangeEvent event = received.get(45, TimeUnit.SECONDS);
        assertEquals(CassandraChangeEvent.Operation.INSERT, event.getOperation());

        Map<String, Object> after = event.getAfter();
        assertEquals(101, ((Number) after.get("id")).intValue());
      } finally {
        registration.subscription().cancel();
        stream.close();
        vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
      }
    }
  }
}
