package dev.henneberger.vertx.mysql.replication;

import dev.henneberger.vertx.replication.core.RetryPolicy;
import io.vertx.core.json.JsonObject;
import java.time.Duration;

final class MySqlReplicationOptionsConverter {

  private MySqlReplicationOptionsConverter() {
  }

  static void fromJson(JsonObject json, MySqlReplicationOptions options) {
    if (json == null) {
      return;
    }
    if (json.containsKey("host")) options.setHost(json.getString("host"));
    if (json.containsKey("port")) options.setPort(json.getInteger("port"));
    if (json.containsKey("database")) options.setDatabase(json.getString("database"));
    if (json.containsKey("user")) options.setUser(json.getString("user"));
    if (json.containsKey("password")) options.setPassword(json.getString("password"));
    if (json.containsKey("passwordEnv")) options.setPasswordEnv(json.getString("passwordEnv"));
    if (json.containsKey("serverId")) options.setServerId(json.getLong("serverId"));
    if (json.containsKey("connectTimeoutMs")) options.setConnectTimeoutMs(json.getLong("connectTimeoutMs"));
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

  static void toJson(MySqlReplicationOptions options, JsonObject json) {
    json.put("host", options.getHost());
    json.put("port", options.getPort());
    json.put("database", options.getDatabase());
    json.put("user", options.getUser());
    json.put("password", options.getPassword());
    json.put("passwordEnv", options.getPasswordEnv());
    json.put("serverId", options.getServerId());
    json.put("connectTimeoutMs", options.getConnectTimeoutMs());
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
