package dev.henneberger.vertx.mongodb.replication;

import dev.henneberger.vertx.replication.core.RetryPolicy;
import io.vertx.core.json.JsonObject;
import java.time.Duration;

final class MongoDbReplicationOptionsConverter {

  private MongoDbReplicationOptionsConverter() {
  }

  static void fromJson(JsonObject json, MongoDbReplicationOptions options) {
    if (json == null) {
      return;
    }
    if (json.containsKey("connectionString")) {
      options.setConnectionString(json.getString("connectionString"));
    }
    if (json.containsKey("database")) {
      options.setDatabase(json.getString("database"));
    }
    if (json.containsKey("collection")) {
      options.setCollection(json.getString("collection"));
    }
    if (json.containsKey("fullDocumentLookup")) {
      options.setFullDocumentLookup(json.getBoolean("fullDocumentLookup"));
    }
    if (json.containsKey("batchSize")) {
      options.setBatchSize(json.getInteger("batchSize"));
    }
    if (json.containsKey("preflightEnabled")) {
      options.setPreflightEnabled(json.getBoolean("preflightEnabled"));
    }
    if (json.containsKey("autoStart")) {
      options.setAutoStart(json.getBoolean("autoStart"));
    }
    if (json.containsKey("maxConcurrentDispatch")) {
      options.setMaxConcurrentDispatch(json.getInteger("maxConcurrentDispatch"));
    }

    JsonObject retryPolicyJson = json.getJsonObject("retryPolicy");
    if (retryPolicyJson != null) {
      RetryPolicy parsed = RetryPolicy.exponentialBackoff();
      parsed.setInitialDelay(Duration.ofMillis(retryPolicyJson.getLong("initialDelayMs", 1000L)));
      parsed.setMaxDelay(Duration.ofMillis(retryPolicyJson.getLong("maxDelayMs", 30000L)));
      parsed.setMultiplier(retryPolicyJson.getDouble("multiplier", 2.0d));
      parsed.setJitter(retryPolicyJson.getDouble("jitter", 0.2d));
      parsed.setMaxAttempts(retryPolicyJson.getLong("maxAttempts", 0L));
      options.setRetryPolicy(parsed);
    }
  }

  static void toJson(MongoDbReplicationOptions options, JsonObject json) {
    json.put("connectionString", options.getConnectionString());
    json.put("database", options.getDatabase());
    json.put("collection", options.getCollection());
    json.put("fullDocumentLookup", options.isFullDocumentLookup());
    json.put("batchSize", options.getBatchSize());
    json.put("preflightEnabled", options.isPreflightEnabled());
    json.put("autoStart", options.isAutoStart());
    json.put("maxConcurrentDispatch", options.getMaxConcurrentDispatch());

    RetryPolicy retryPolicy = options.getRetryPolicy();
    if (retryPolicy != null && retryPolicy.isEnabled()) {
      JsonObject retry = new JsonObject();
      retry.put("initialDelayMs", retryPolicy.getInitialDelay().toMillis());
      retry.put("maxDelayMs", retryPolicy.getMaxDelay().toMillis());
      retry.put("multiplier", retryPolicy.getMultiplier());
      retry.put("jitter", retryPolicy.getJitter());
      retry.put("maxAttempts", retryPolicy.getMaxAttempts());
      json.put("retryPolicy", retry);
    }
  }
}
