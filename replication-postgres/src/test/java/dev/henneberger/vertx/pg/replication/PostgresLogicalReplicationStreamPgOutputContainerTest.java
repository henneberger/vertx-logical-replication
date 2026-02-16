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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import dev.henneberger.vertx.replication.core.SubscriptionRegistration;
import io.vertx.core.Vertx;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
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
