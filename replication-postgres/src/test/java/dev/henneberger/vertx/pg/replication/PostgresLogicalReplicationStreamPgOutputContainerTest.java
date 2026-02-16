/*
 * Copyright (C) 2026 Daniel Henneberger
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.henneberger.vertx.pg.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.henneberger.vertx.replication.core.SubscriptionRegistration;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

class PostgresLogicalReplicationStreamPgOutputContainerTest {

  private static final String SLOT_NAME = "vertx_pgoutput_slot";
  private static final String PUBLICATION_NAME = "vertx_pgoutput_pub";
  private static final String DB_NAME = "testdb";
  private static final String DB_USER = "test";
  private static final String DB_PASSWORD = "test";

  @Test
  void streamsInsertFromPgOutput() throws Exception {
    Assumptions.assumeTrue(
      DockerClientFactory.instance().isDockerAvailable(),
      "Docker is required for Testcontainers integration tests");

    GenericContainer<?> postgres = createPostgresContainer();
    try {
      postgres.start();
      try (Connection conn = DriverManager.getConnection(jdbcUrl(postgres), DB_USER, DB_PASSWORD);
           Statement statement = conn.createStatement()) {
        statement.execute("CREATE TABLE IF NOT EXISTS messages (id SERIAL PRIMARY KEY, body TEXT NOT NULL)");
        statement.execute("DROP PUBLICATION IF EXISTS " + PUBLICATION_NAME);
        statement.execute("CREATE PUBLICATION " + PUBLICATION_NAME + " FOR TABLE messages");
      }

      Vertx vertx = Vertx.vertx();
      PostgresLogicalReplicationStream stream = new PostgresLogicalReplicationStream(
        vertx,
        new PostgresReplicationOptions()
          .setHost(postgres.getHost())
          .setPort(postgres.getFirstMappedPort())
          .setDatabase(DB_NAME)
          .setUser(DB_USER)
          .setPassword(DB_PASSWORD)
          .setSlotName(SLOT_NAME)
          .setPlugin("pgoutput")
          .setPublicationName(PUBLICATION_NAME)
          .setChangeDecoder(new PgOutputChangeDecoder()));

      CompletableFuture<PostgresChangeEvent> received = new CompletableFuture<>();
      SubscriptionRegistration registration = stream.startAndSubscribe(
        PostgresChangeFilter.tables("public.messages"),
        event -> {
          if (event.getOperation() == PostgresChangeEvent.Operation.INSERT) {
            received.complete(event);
          }
        },
        received::completeExceptionally
      );

      try {
        registration.started().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
        waitForSlotCreation(postgres, SLOT_NAME);
        try (Connection conn = DriverManager.getConnection(jdbcUrl(postgres), DB_USER, DB_PASSWORD);
             Statement statement = conn.createStatement()) {
          statement.execute("INSERT INTO messages(body) VALUES ('hello-pgoutput')");
        }

        PostgresChangeEvent event = received.get(30, TimeUnit.SECONDS);
        assertNotNull(event);
        assertEquals("public.messages", event.getTable());
        assertEquals(PostgresChangeEvent.Operation.INSERT, event.getOperation());
        assertEquals("hello-pgoutput", event.getNewData().get("body"));
      } finally {
        registration.subscription().cancel();
        stream.close();
        vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
      }
    } finally {
      postgres.stop();
    }
  }

  @Test
  void streamsInsertUpdateDeleteWithCommonTypesFromPgOutput() throws Exception {
    Assumptions.assumeTrue(
      DockerClientFactory.instance().isDockerAvailable(),
      "Docker is required for Testcontainers integration tests");

    GenericContainer<?> postgres = createPostgresContainer();
    try {
      postgres.start();
      try (Connection conn = DriverManager.getConnection(jdbcUrl(postgres), DB_USER, DB_PASSWORD);
           Statement statement = conn.createStatement()) {
        statement.execute("CREATE TABLE IF NOT EXISTS cdc_types (" +
          "id BIGSERIAL PRIMARY KEY," +
          "body TEXT NOT NULL," +
          "active BOOLEAN NOT NULL," +
          "score INTEGER NOT NULL," +
          "amount NUMERIC(10,2) NOT NULL," +
          "meta JSONB NOT NULL," +
          "tags JSONB NOT NULL)");
        statement.execute("ALTER TABLE cdc_types REPLICA IDENTITY FULL");
        statement.execute("DROP PUBLICATION IF EXISTS " + PUBLICATION_NAME);
        statement.execute("CREATE PUBLICATION " + PUBLICATION_NAME + " FOR TABLE cdc_types");
      }

      Vertx vertx = Vertx.vertx();
      PostgresLogicalReplicationStream stream = new PostgresLogicalReplicationStream(
        vertx,
        new PostgresReplicationOptions()
          .setHost(postgres.getHost())
          .setPort(postgres.getFirstMappedPort())
          .setDatabase(DB_NAME)
          .setUser(DB_USER)
          .setPassword(DB_PASSWORD)
          .setSlotName(SLOT_NAME + "_types")
          .setPlugin("pgoutput")
          .setPublicationName(PUBLICATION_NAME)
          .setChangeDecoder(new PgOutputChangeDecoder()));

      BlockingQueue<PostgresChangeEvent> events = new LinkedBlockingQueue<>();
      SubscriptionRegistration registration = stream.startAndSubscribe(
        PostgresChangeFilter.tables("public.cdc_types"),
        event -> {
          events.offer(event);
          return Future.succeededFuture();
        },
        err -> {
          throw new RuntimeException(err);
        }
      );

      try {
        registration.started().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
        waitForSlotCreation(postgres, SLOT_NAME + "_types");
        try (Connection conn = DriverManager.getConnection(jdbcUrl(postgres), DB_USER, DB_PASSWORD);
             Statement statement = conn.createStatement()) {
          statement.execute(
            "INSERT INTO cdc_types(body, active, score, amount, meta, tags) VALUES (" +
              "'hello-types', true, 7, 12.34, " +
              "'{\"a\":1,\"nested\":{\"b\":\"x\"}}'::jsonb, " +
              "'[\"alpha\",\"beta\"]'::jsonb)");
          statement.execute(
            "UPDATE cdc_types SET body='hello-updated', active=false, score=8, amount=45.67, " +
              "meta='{\"a\":2,\"extra\":true}'::jsonb, tags='[\"gamma\",3]'::jsonb WHERE id=1");
          statement.execute("DELETE FROM cdc_types WHERE id=1");
        }

        PostgresChangeEvent insert = poll(events, "insert");
        PostgresChangeEvent update = poll(events, "update");
        PostgresChangeEvent delete = poll(events, "delete");

        assertEquals(PostgresChangeEvent.Operation.INSERT, insert.getOperation());
        assertEquals("public.cdc_types", insert.getTable());
        assertEquals("hello-types", insert.getNewData().get("body"));
        assertEquals(true, insert.getNewData().get("active"));
        assertEquals(7, ((Number) insert.getNewData().get("score")).intValue());
        assertEquals(12.34d, ((Number) insert.getNewData().get("amount")).doubleValue(), 0.0001d);
        assertTrue(insert.getNewData().get("meta") instanceof Map);
        assertTrue(insert.getNewData().get("tags") instanceof List);
        Map<?, ?> insertMeta = (Map<?, ?>) insert.getNewData().get("meta");
        assertEquals(1, ((Number) insertMeta.get("a")).intValue());

        assertEquals(PostgresChangeEvent.Operation.UPDATE, update.getOperation());
        assertEquals("hello-updated", update.getNewData().get("body"));
        assertEquals(false, update.getNewData().get("active"));
        assertEquals(8, ((Number) update.getNewData().get("score")).intValue());
        assertEquals(45.67d, ((Number) update.getNewData().get("amount")).doubleValue(), 0.0001d);
        assertTrue(update.getNewData().get("meta") instanceof Map);
        assertTrue(update.getNewData().get("tags") instanceof List);
        assertFalse(update.getOldKeys().isEmpty(), "UPDATE should include key columns in oldKeys");
        assertEquals(1L, ((Number) update.getOldKeys().get("id")).longValue());

        assertEquals(PostgresChangeEvent.Operation.DELETE, delete.getOperation());
        assertTrue(delete.getNewData().isEmpty());
        assertEquals(1L, ((Number) delete.getOldKeys().get("id")).longValue());
      } finally {
        registration.subscription().cancel();
        stream.close();
        vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
      }
    } finally {
      postgres.stop();
    }
  }

  private static PostgresChangeEvent poll(BlockingQueue<PostgresChangeEvent> events, String label) throws Exception {
    PostgresChangeEvent event = events.poll(30, TimeUnit.SECONDS);
    if (event == null) {
      throw new IllegalStateException("Timed out waiting for " + label + " event");
    }
    return event;
  }

  private static void waitForSlotCreation(GenericContainer<?> postgres, String slotName) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (System.nanoTime() < deadline) {
      try (Connection conn = DriverManager.getConnection(jdbcUrl(postgres), DB_USER, DB_PASSWORD);
           Statement statement = conn.createStatement();
           ResultSet rs = statement.executeQuery(
             "SELECT slot_name FROM pg_replication_slots WHERE slot_name='" + slotName + "'")) {
        if (rs.next()) {
          return;
        }
      }
      Thread.sleep(200);
    }
    throw new IllegalStateException("Replication slot was not created: " + slotName);
  }

  private static GenericContainer<?> createPostgresContainer() {
    return new GenericContainer<>("postgres:16")
      .withExposedPorts(5432)
      .withEnv("POSTGRES_DB", DB_NAME)
      .withEnv("POSTGRES_USER", DB_USER)
      .withEnv("POSTGRES_PASSWORD", DB_PASSWORD)
      .withStartupTimeout(Duration.ofMinutes(10))
      .withCommand("postgres",
        "-c", "wal_level=logical",
        "-c", "max_replication_slots=10",
        "-c", "max_wal_senders=10");
  }

  private static String jdbcUrl(GenericContainer<?> postgres) {
    return "jdbc:postgresql://" + postgres.getHost() + ":" + postgres.getFirstMappedPort() + "/" + DB_NAME;
  }
}
