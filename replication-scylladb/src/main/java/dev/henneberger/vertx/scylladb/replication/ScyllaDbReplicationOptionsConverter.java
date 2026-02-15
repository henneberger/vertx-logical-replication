package dev.henneberger.vertx.scylladb.replication;

import dev.henneberger.vertx.replication.core.RetryPolicy;
import io.vertx.core.json.JsonObject;
import java.time.Duration;

final class ScyllaDbReplicationOptionsConverter {

  private ScyllaDbReplicationOptionsConverter() {
  }

  static void fromJson(JsonObject json, ScyllaDbReplicationOptions options) {
    if (json == null) {
      return;
    }
    if (json.containsKey("host")) options.setHost(json.getString("host"));
    if (json.containsKey("port")) options.setPort(json.getInteger("port"));
    if (json.containsKey("localDatacenter")) options.setLocalDatacenter(json.getString("localDatacenter"));
    if (json.containsKey("keyspace")) options.setKeyspace(json.getString("keyspace"));
    if (json.containsKey("sourceTable")) options.setSourceTable(json.getString("sourceTable"));
    if (json.containsKey("user")) options.setUser(json.getString("user"));
    if (json.containsKey("password")) options.setPassword(json.getString("password"));
    if (json.containsKey("passwordEnv")) options.setPasswordEnv(json.getString("passwordEnv"));
    if (json.containsKey("positionColumn")) options.setPositionColumn(json.getString("positionColumn"));
    if (json.containsKey("operationColumn")) options.setOperationColumn(json.getString("operationColumn"));
    if (json.containsKey("pollIntervalMs")) options.setPollIntervalMs(json.getLong("pollIntervalMs"));
    if (json.containsKey("batchSize")) options.setBatchSize(json.getInteger("batchSize"));
    if (json.containsKey("preflightEnabled")) options.setPreflightEnabled(json.getBoolean("preflightEnabled"));
    if (json.containsKey("autoStart")) options.setAutoStart(json.getBoolean("autoStart"));
    if (json.containsKey("maxConcurrentDispatch")) options.setMaxConcurrentDispatch(json.getInteger("maxConcurrentDispatch"));

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

  static void toJson(ScyllaDbReplicationOptions options, JsonObject json) {
    json.put("host", options.getHost());
    json.put("port", options.getPort());
    json.put("localDatacenter", options.getLocalDatacenter());
    json.put("keyspace", options.getKeyspace());
    json.put("sourceTable", options.getSourceTable());
    json.put("user", options.getUser());
    json.put("password", options.getPassword());
    json.put("passwordEnv", options.getPasswordEnv());
    json.put("positionColumn", options.getPositionColumn());
    json.put("operationColumn", options.getOperationColumn());
    json.put("pollIntervalMs", options.getPollIntervalMs());
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
