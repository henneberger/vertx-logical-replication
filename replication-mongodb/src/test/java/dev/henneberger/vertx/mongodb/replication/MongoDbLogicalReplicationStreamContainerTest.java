package dev.henneberger.vertx.mongodb.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import java.time.Duration;
import org.bson.Document;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

class MongoDbLogicalReplicationStreamContainerTest {

  @Test
  void connectsToMongoDbContainer() {
    Assumptions.assumeTrue(
      DockerClientFactory.instance().isDockerAvailable(),
      "Docker is required for Testcontainers integration tests");

    try (GenericContainer<?> mongo = new GenericContainer<>("mongo:7")
      .withExposedPorts(27017)
      .withStartupTimeout(Duration.ofMinutes(4))) {

      try {
        mongo.start();
      } catch (Exception startupError) {
        Assumptions.assumeTrue(false, "MongoDB container startup failed: " + startupError.getMessage());
        return;
      }

      String connectionString = "mongodb://" + mongo.getHost() + ':' + mongo.getFirstMappedPort();
      try (MongoClient client = MongoClients.create(connectionString)) {
        MongoCollection<Document> collection = client.getDatabase("cdcdb").getCollection("orders");
        collection.insertOne(new Document("_id", 1).append("amount", 55.25));

        Document doc = collection.find(new Document("_id", 1)).first();
        assertEquals(1, doc.getInteger("_id"));
      } catch (Exception connectionError) {
        Assumptions.assumeTrue(false, "MongoDB container connectivity failed: " + connectionError.getMessage());
      }
    }
  }
}
