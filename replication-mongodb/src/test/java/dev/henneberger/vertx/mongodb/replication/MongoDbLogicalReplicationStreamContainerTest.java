package dev.henneberger.vertx.mongodb.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import dev.henneberger.vertx.replication.core.RetryPolicy;
import dev.henneberger.vertx.replication.core.SubscriptionRegistration;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

class MongoDbLogicalReplicationStreamContainerTest {

  @Test
  void streamsInsertFromMongoDbChangeStream() throws Exception {
    Assumptions.assumeTrue(
      DockerClientFactory.instance().isDockerAvailable(),
      "Docker is required for Testcontainers integration tests");

    try (GenericContainer<?> mongo = new GenericContainer<>("mongo:7")
      .withExposedPorts(27017)
      .withCommand("--replSet", "rs0", "--bind_ip_all")
      .withStartupTimeout(Duration.ofMinutes(4))) {

      try {
        mongo.start();
      } catch (Exception startupError) {
        Assumptions.assumeTrue(false, "MongoDB container startup failed: " + startupError.getMessage());
        return;
      }

      try {
        Container.ExecResult rsInit = mongo.execInContainer(
          "mongosh",
          "--quiet",
          "--eval",
          "try { rs.initiate({_id:'rs0', members:[{_id:0, host:'localhost:27017'}]}) } catch(e) {}");
        if (rsInit.getExitCode() != 0) {
          Assumptions.assumeTrue(false, "MongoDB replica set init failed: " + rsInit.getStderr());
          return;
        }
      } catch (Exception initError) {
        Assumptions.assumeTrue(false, "MongoDB replica set init failed: " + initError.getMessage());
        return;
      }

      boolean primaryReady = false;
      for (int i = 0; i < 30; i++) {
        Container.ExecResult status = mongo.execInContainer(
          "mongosh",
          "--quiet",
          "--eval",
          "db.hello().isWritablePrimary ? 'ready' : 'wait'");
        if (status.getExitCode() == 0 && status.getStdout().contains("ready")) {
          primaryReady = true;
          break;
        }
        Thread.sleep(1000);
      }
      Assumptions.assumeTrue(primaryReady, "MongoDB replica set did not become writable primary");

      String connectionString = "mongodb://" + mongo.getHost() + ':' + mongo.getFirstMappedPort()
        + "/?replicaSet=rs0&directConnection=true";

      Vertx vertx = Vertx.vertx();
      MongoDbLogicalReplicationStream stream = new MongoDbLogicalReplicationStream(
        vertx,
        new MongoDbReplicationOptions()
          .setConnectionString(connectionString)
          .setDatabase("cdcdb")
          .setCollection("orders")
          .setBatchSize(128)
          .setRetryPolicy(RetryPolicy.disabled())
          .setPreflightEnabled(true));

      CompletableFuture<MongoDbChangeEvent> received = new CompletableFuture<>();
      SubscriptionRegistration registration = stream.startAndSubscribe(
        MongoDbChangeFilter.all().operations(MongoDbChangeEvent.Operation.INSERT),
        event -> {
          received.complete(event);
          return Future.succeededFuture();
        },
        received::completeExceptionally
      );

      try {
        registration.started().toCompletionStage().toCompletableFuture().get(40, TimeUnit.SECONDS);

        try (MongoClient client = MongoClients.create(connectionString)) {
          MongoCollection<Document> collection = client.getDatabase("cdcdb").getCollection("orders");
          collection.insertOne(new Document("_id", 101).append("amount", 55.25));
        }

        MongoDbChangeEvent event = received.get(40, TimeUnit.SECONDS);
        assertEquals(MongoDbChangeEvent.Operation.INSERT, event.getOperation());
        Map<String, Object> after = event.getAfter();
        assertEquals(101, ((Number) after.get("_id")).intValue());
      } finally {
        registration.subscription().cancel();
        stream.close();
        vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
      }
    }
  }
}
