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

import dev.henneberger.vertx.replication.core.RetryPolicy;
import io.vertx.core.json.JsonObject;
import java.time.Duration;
import java.util.Map;

final class PostgresReplicationOptionsConverter {

  private PostgresReplicationOptionsConverter() {
  }

  static void fromJson(JsonObject json, PostgresReplicationOptions options) {
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
    if (json.containsKey("slotName")) {
      options.setSlotName(json.getString("slotName"));
    }
    if (json.containsKey("publicationName")) {
      options.setPublicationName(json.getString("publicationName"));
    }
    if (json.containsKey("plugin")) {
      options.setPlugin(json.getString("plugin"));
    }

    JsonObject pluginOptionsJson = json.getJsonObject("pluginOptions");
    if (pluginOptionsJson != null) {
      options.setPluginOptions(pluginOptionsJson.getMap());
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

  static void toJson(PostgresReplicationOptions options, JsonObject json) {
    json.put("host", options.getHost());
    json.put("port", options.getPort());
    json.put("database", options.getDatabase());
    json.put("user", options.getUser());
    json.put("password", options.getPassword());
    json.put("passwordEnv", options.getPasswordEnv());
    json.put("ssl", options.getSsl());
    json.put("slotName", options.getSlotName());
    json.put("publicationName", options.getPublicationName());
    json.put("plugin", options.getPlugin());

    Map<String, Object> pluginOptions = options.getPluginOptions();
    json.put("pluginOptions", pluginOptions == null ? new JsonObject() : new JsonObject(pluginOptions));

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
