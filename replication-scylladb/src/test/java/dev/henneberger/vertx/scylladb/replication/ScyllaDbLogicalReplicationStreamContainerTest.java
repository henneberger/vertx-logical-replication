package dev.henneberger.vertx.scylladb.replication;

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

class ScyllaDbLogicalReplicationStreamContainerTest {

  @Test
  void streamsInsertFromScyllaDbCdcTable() throws Exception {
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
        session.execute("CREATE TABLE IF NOT EXISTS cdc.cdc_events (position bigint PRIMARY KEY, operation text, id int, amount decimal)");
      }

      Vertx vertx = Vertx.vertx();
      ScyllaDbLogicalReplicationStream stream = new ScyllaDbLogicalReplicationStream(
        vertx,
        new ScyllaDbReplicationOptions()
          .setHost(scylla.getHost())
          .setPort(scylla.getFirstMappedPort())
          .setLocalDatacenter("datacenter1")
          .setKeyspace("cdc")
          .setSourceTable("cdc_events")
          .setPollIntervalMs(120)
          .setBatchSize(50)
          .setRetryPolicy(RetryPolicy.disabled())
          .setPreflightEnabled(true));

      CompletableFuture<ScyllaDbChangeEvent> received = new CompletableFuture<>();
      SubscriptionRegistration registration = stream.startAndSubscribe(
        ScyllaDbChangeFilter.all().operations(ScyllaDbChangeEvent.Operation.INSERT),
        event -> {
          received.complete(event);
          return Future.succeededFuture();
        },
        received::completeExceptionally
      );

      try {
        registration.started().toCompletionStage().toCompletableFuture().get(35, TimeUnit.SECONDS);

        try (CqlSession session = CqlSession.builder()
          .addContactPoint(new InetSocketAddress(scylla.getHost(), scylla.getFirstMappedPort()))
          .withLocalDatacenter("datacenter1")
          .withKeyspace("cdc")
          .build()) {
          session.execute(SimpleStatement.newInstance(
            "INSERT INTO cdc_events(position, operation, id, amount) VALUES (?, ?, ?, ?)",
            1L, "INSERT", 101, java.math.BigDecimal.valueOf(55.25)));
        }

        ScyllaDbChangeEvent event = received.get(45, TimeUnit.SECONDS);
        assertEquals(ScyllaDbChangeEvent.Operation.INSERT, event.getOperation());

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
