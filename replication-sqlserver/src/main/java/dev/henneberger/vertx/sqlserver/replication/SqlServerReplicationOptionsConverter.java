package dev.henneberger.vertx.sqlserver.replication;

import dev.henneberger.vertx.replication.core.RetryPolicy;
import io.vertx.core.json.JsonObject;
import java.time.Duration;

final class SqlServerReplicationOptionsConverter {

  private SqlServerReplicationOptionsConverter() {
  }

  static void fromJson(JsonObject json, SqlServerReplicationOptions options) {
    if (json == null) {
      return;
    }
    if (json.containsKey("host")) {
      options.setHost(json.getString("host"));
    }
    if (json.containsKey("port")) {
      options.setPort(json.getInteger("port"));
    }
    if (json.containsKey("database")) {
      options.setDatabase(json.getString("database"));
    }
    if (json.containsKey("user")) {
      options.setUser(json.getString("user"));
    }
    if (json.containsKey("password")) {
      options.setPassword(json.getString("password"));
    }
    if (json.containsKey("passwordEnv")) {
      options.setPasswordEnv(json.getString("passwordEnv"));
    }
    if (json.containsKey("ssl")) {
      options.setSsl(json.getBoolean("ssl"));
    }
    if (json.containsKey("captureInstance")) {
      options.setCaptureInstance(json.getString("captureInstance"));
    }
    if (json.containsKey("pollIntervalMs")) {
      options.setPollIntervalMs(json.getLong("pollIntervalMs"));
    }
    if (json.containsKey("maxBatchSize")) {
      options.setMaxBatchSize(json.getInteger("maxBatchSize"));
    }
    if (json.containsKey("preflightEnabled")) {
      options.setPreflightEnabled(json.getBoolean("preflightEnabled"));
    }
    if (json.containsKey("preflightMode")) {
      options.setPreflightMode(json.getString("preflightMode"));
    }
    if (json.containsKey("preflightMaxWaitMs")) {
      options.setPreflightMaxWaitMs(json.getLong("preflightMaxWaitMs"));
    }
    if (json.containsKey("preflightRetryIntervalMs")) {
      options.setPreflightRetryIntervalMs(json.getLong("preflightRetryIntervalMs"));
    }
    if (json.containsKey("autoStart")) {
      options.setAutoStart(json.getBoolean("autoStart"));
    }
    if (json.containsKey("maxConcurrentDispatch")) {
      options.setMaxConcurrentDispatch(json.getInteger("maxConcurrentDispatch"));
    }
    if (json.containsKey("valueNormalizationMode")) {
      options.setValueNormalizationMode(json.getString("valueNormalizationMode"));
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

  static void toJson(SqlServerReplicationOptions options, JsonObject json) {
    json.put("host", options.getHost());
    json.put("port", options.getPort());
    json.put("database", options.getDatabase());
    json.put("user", options.getUser());
    json.put("password", options.getPassword());
    json.put("passwordEnv", options.getPasswordEnv());
    json.put("ssl", options.getSsl());
    json.put("captureInstance", options.getCaptureInstance());
    json.put("pollIntervalMs", options.getPollIntervalMs());
    json.put("maxBatchSize", options.getMaxBatchSize());
    json.put("preflightEnabled", options.isPreflightEnabled());
    json.put("preflightMode", options.getPreflightMode());
    json.put("preflightMaxWaitMs", options.getPreflightMaxWaitMs());
    json.put("preflightRetryIntervalMs", options.getPreflightRetryIntervalMs());
    json.put("autoStart", options.isAutoStart());
    json.put("maxConcurrentDispatch", options.getMaxConcurrentDispatch());
    json.put("valueNormalizationMode", options.getValueNormalizationMode());

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
