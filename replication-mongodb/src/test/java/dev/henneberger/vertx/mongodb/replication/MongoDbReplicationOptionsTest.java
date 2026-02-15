package dev.henneberger.vertx.mongodb.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

class MongoDbReplicationOptionsTest {

  @Test
  void readsAndSerializesJson() {
    MongoDbReplicationOptions options = new MongoDbReplicationOptions(new JsonObject()
      .put("connectionString", "mongodb://mongo.internal:27017")
      .put("database", "app")
      .put("collection", "orders")
      .put("fullDocumentLookup", false)
      .put("batchSize", 128)
      .put("maxConcurrentDispatch", 2));

    assertEquals("mongodb://mongo.internal:27017", options.getConnectionString());
    assertEquals("app", options.getDatabase());
    assertEquals("orders", options.getCollection());
    assertEquals(128, options.getBatchSize());

    JsonObject json = options.toJson();
    assertEquals("orders", json.getString("collection"));
    assertEquals(2, json.getInteger("maxConcurrentDispatch"));
  }

  @Test
  void validatesRequiredFields() {
    MongoDbReplicationOptions options = new MongoDbReplicationOptions()
      .setConnectionString("mongodb://localhost:27017")
      .setDatabase("db")
      .setCollection("orders");

    options.validate();
    assertTrue(options.isAutoStart());

    assertThrows(IllegalArgumentException.class, () -> options.setBatchSize(0).validate());
  }
}
