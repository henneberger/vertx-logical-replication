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

import dev.henneberger.vertx.replication.core.LsnStore;
import dev.henneberger.vertx.replication.core.NoopLsnStore;
import dev.henneberger.vertx.replication.core.RetryPolicy;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.json.annotations.JsonGen;
import io.vertx.core.json.JsonObject;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Connection and replication slot configuration.
 */
@DataObject
@JsonGen(publicConverter = false)
public class PostgresReplicationOptions {

  public static final String DEFAULT_HOST = "localhost";
  public static final int DEFAULT_PORT = 5432;
  public static final String DEFAULT_PLUGIN = "wal2json";

  private String host;
  private int port;
  private String database;
  private String user;
  private String password;
  private String passwordEnv;
  private boolean ssl;
  private String slotName;

  private String plugin = DEFAULT_PLUGIN;
  private Map<String, Object> pluginOptions = new LinkedHashMap<>();
  private RetryPolicy retryPolicy = RetryPolicy.disabled();
  private boolean preflightEnabled;
  private boolean autoStart = true;
  private LsnStore lsnStore = new NoopLsnStore();
  private ChangeDecoder changeDecoder = new Wal2JsonChangeDecoder();
  private int maxConcurrentDispatch = 1;

  public PostgresReplicationOptions() {
    init();
  }

  public PostgresReplicationOptions(JsonObject json) {
    init();
    PostgresReplicationOptionsConverter.fromJson(json, this);
  }

  public PostgresReplicationOptions(PostgresReplicationOptions other) {
    this.host = other.host;
    this.port = other.port;
    this.database = other.database;
    this.user = other.user;
    this.password = other.password;
    this.passwordEnv = other.passwordEnv;
    this.ssl = other.ssl;
    this.slotName = other.slotName;
    this.plugin = other.plugin;
    this.pluginOptions = new LinkedHashMap<>(other.pluginOptions);
    this.retryPolicy = other.retryPolicy.copy();
    this.preflightEnabled = other.preflightEnabled;
    this.autoStart = other.autoStart;
    this.lsnStore = other.lsnStore;
    this.changeDecoder = other.changeDecoder;
    this.maxConcurrentDispatch = other.maxConcurrentDispatch;
  }

  public String getHost() {
    return host;
  }

  public PostgresReplicationOptions setHost(String host) {
    this.host = host;
    return this;
  }

  public Integer getPort() {
    return port;
  }

  public PostgresReplicationOptions setPort(Integer port) {
    this.port = port;
    return this;
  }

  public String getDatabase() {
    return database;
  }

  public PostgresReplicationOptions setDatabase(String database) {
    this.database = database;
    return this;
  }

  public String getUser() {
    return user;
  }

  public PostgresReplicationOptions setUser(String user) {
    this.user = user;
    return this;
  }

  public String getPassword() {
    return password;
  }

  public PostgresReplicationOptions setPassword(String password) {
    this.password = password;
    return this;
  }

  public String getPasswordEnv() {
    return passwordEnv;
  }

  public PostgresReplicationOptions setPasswordEnv(String passwordEnv) {
    this.passwordEnv = passwordEnv;
    return this;
  }

  public Boolean getSsl() {
    return ssl;
  }

  public PostgresReplicationOptions setSsl(Boolean ssl) {
    this.ssl = Boolean.TRUE.equals(ssl);
    return this;
  }

  public String getSlotName() {
    return slotName;
  }

  public PostgresReplicationOptions setSlotName(String slotName) {
    this.slotName = slotName;
    return this;
  }

  public String getPlugin() {
    return plugin;
  }

  public PostgresReplicationOptions setPlugin(String plugin) {
    this.plugin = plugin;
    return this;
  }

  public Map<String, Object> getPluginOptions() {
    return Collections.unmodifiableMap(pluginOptions);
  }

  public PostgresReplicationOptions setPluginOptions(Map<String, Object> options) {
    this.pluginOptions = options == null ? new LinkedHashMap<>() : new LinkedHashMap<>(options);
    return this;
  }

  public RetryPolicy getRetryPolicy() {
    return retryPolicy;
  }

  @GenIgnore
  public PostgresReplicationOptions setRetryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
    return this;
  }

  public boolean isPreflightEnabled() {
    return preflightEnabled;
  }

  public PostgresReplicationOptions setPreflightEnabled(boolean preflightEnabled) {
    this.preflightEnabled = preflightEnabled;
    return this;
  }

  public boolean isAutoStart() {
    return autoStart;
  }

  public PostgresReplicationOptions setAutoStart(boolean autoStart) {
    this.autoStart = autoStart;
    return this;
  }

  public int getMaxConcurrentDispatch() {
    return maxConcurrentDispatch;
  }

  public PostgresReplicationOptions setMaxConcurrentDispatch(int maxConcurrentDispatch) {
    this.maxConcurrentDispatch = maxConcurrentDispatch;
    return this;
  }

  @GenIgnore
  public LsnStore getLsnStore() {
    return lsnStore;
  }

  @GenIgnore
  public PostgresReplicationOptions setLsnStore(LsnStore lsnStore) {
    this.lsnStore = Objects.requireNonNull(lsnStore, "lsnStore");
    return this;
  }

  @GenIgnore
  public ChangeDecoder getChangeDecoder() {
    return changeDecoder;
  }

  @GenIgnore
  public PostgresReplicationOptions setChangeDecoder(ChangeDecoder changeDecoder) {
    this.changeDecoder = Objects.requireNonNull(changeDecoder, "changeDecoder");
    return this;
  }

  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    PostgresReplicationOptionsConverter.toJson(this, json);
    return json;
  }

  public PostgresReplicationOptions merge(JsonObject other) {
    JsonObject json = toJson();
    json.mergeIn(other);
    return new PostgresReplicationOptions(json);
  }

  void validate() {
    require("host", host);
    if (port < 1 || port > 65535) {
      throw new IllegalArgumentException("port must be between 1 and 65535");
    }
    require("database", database);
    require("user", user);
    require("slotName", slotName);
    require("plugin", plugin);
    Objects.requireNonNull(pluginOptions, "pluginOptions");
    Objects.requireNonNull(retryPolicy, "retryPolicy").validate();
    Objects.requireNonNull(lsnStore, "lsnStore");
    ChangeDecoder decoder = Objects.requireNonNull(changeDecoder, "changeDecoder");
    if (!decoder.supportsPlugin(plugin)) {
      throw new IllegalArgumentException(
        "changeDecoder " + decoder.getClass().getSimpleName() + " does not support plugin " + plugin);
    }
    if (maxConcurrentDispatch < 1) {
      throw new IllegalArgumentException("maxConcurrentDispatch must be >= 1");
    }
  }

  private static void require(String fieldName, String value) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(fieldName + " is required");
    }
  }

  private void init() {
    host = DEFAULT_HOST;
    port = DEFAULT_PORT;
    ssl = false;
    plugin = DEFAULT_PLUGIN;
    pluginOptions = new LinkedHashMap<>();
    retryPolicy = RetryPolicy.disabled();
    preflightEnabled = false;
    autoStart = true;
    lsnStore = new NoopLsnStore();
    changeDecoder = new Wal2JsonChangeDecoder();
    maxConcurrentDispatch = 1;
  }
}
