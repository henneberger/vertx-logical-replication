package dev.henneberger.vertx.neo4j.replication;

import dev.henneberger.vertx.replication.core.LsnStore;
import dev.henneberger.vertx.replication.core.NoopLsnStore;
import dev.henneberger.vertx.replication.core.RetryPolicy;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.json.annotations.JsonGen;
import io.vertx.core.json.JsonObject;
import java.util.Objects;

@DataObject
@JsonGen(publicConverter = false)
public class Neo4jReplicationOptions {

  public static final String DEFAULT_URI = "neo4j://localhost:7687";
  public static final String DEFAULT_QUERY =
    "MATCH (e:CdcEvent) WHERE e.position > $lastPosition "
      + "RETURN coalesce(e.source, $source) AS source, coalesce(e.operation, 'UPDATE') AS operation, "
      + "e.before AS before, e.after AS after, toString(e.position) AS position, e.commitTimestamp AS commitTimestamp "
      + "ORDER BY e.position LIMIT $limit";

  private String uri;
  private String database;
  private String user;
  private String password;
  private String passwordEnv;
  private String sourceName;
  private String eventQuery;
  private long pollIntervalMs;
  private int batchSize;
  private RetryPolicy retryPolicy;
  private boolean preflightEnabled;
  private boolean autoStart;
  private int maxConcurrentDispatch;
  private LsnStore lsnStore;

  public Neo4jReplicationOptions() {
    init();
  }

  public Neo4jReplicationOptions(JsonObject json) {
    init();
    Neo4jReplicationOptionsConverter.fromJson(json, this);
  }

  public Neo4jReplicationOptions(Neo4jReplicationOptions other) {
    this.uri = other.uri;
    this.database = other.database;
    this.user = other.user;
    this.password = other.password;
    this.passwordEnv = other.passwordEnv;
    this.sourceName = other.sourceName;
    this.eventQuery = other.eventQuery;
    this.pollIntervalMs = other.pollIntervalMs;
    this.batchSize = other.batchSize;
    this.retryPolicy = other.retryPolicy.copy();
    this.preflightEnabled = other.preflightEnabled;
    this.autoStart = other.autoStart;
    this.maxConcurrentDispatch = other.maxConcurrentDispatch;
    this.lsnStore = other.lsnStore;
  }

  public String getUri() {
    return uri;
  }

  public Neo4jReplicationOptions setUri(String uri) {
    this.uri = uri;
    return this;
  }

  public String getDatabase() {
    return database;
  }

  public Neo4jReplicationOptions setDatabase(String database) {
    this.database = database;
    return this;
  }

  public String getUser() {
    return user;
  }

  public Neo4jReplicationOptions setUser(String user) {
    this.user = user;
    return this;
  }

  public String getPassword() {
    return password;
  }

  public Neo4jReplicationOptions setPassword(String password) {
    this.password = password;
    return this;
  }

  public String getPasswordEnv() {
    return passwordEnv;
  }

  public Neo4jReplicationOptions setPasswordEnv(String passwordEnv) {
    this.passwordEnv = passwordEnv;
    return this;
  }

  public String getSourceName() {
    return sourceName;
  }

  public Neo4jReplicationOptions setSourceName(String sourceName) {
    this.sourceName = sourceName;
    return this;
  }

  public String getEventQuery() {
    return eventQuery;
  }

  public Neo4jReplicationOptions setEventQuery(String eventQuery) {
    this.eventQuery = eventQuery;
    return this;
  }

  public long getPollIntervalMs() {
    return pollIntervalMs;
  }

  public Neo4jReplicationOptions setPollIntervalMs(long pollIntervalMs) {
    this.pollIntervalMs = pollIntervalMs;
    return this;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public Neo4jReplicationOptions setBatchSize(int batchSize) {
    this.batchSize = batchSize;
    return this;
  }

  public RetryPolicy getRetryPolicy() {
    return retryPolicy;
  }

  @GenIgnore
  public Neo4jReplicationOptions setRetryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
    return this;
  }

  public boolean isPreflightEnabled() {
    return preflightEnabled;
  }

  public Neo4jReplicationOptions setPreflightEnabled(boolean preflightEnabled) {
    this.preflightEnabled = preflightEnabled;
    return this;
  }

  public boolean isAutoStart() {
    return autoStart;
  }

  public Neo4jReplicationOptions setAutoStart(boolean autoStart) {
    this.autoStart = autoStart;
    return this;
  }

  public int getMaxConcurrentDispatch() {
    return maxConcurrentDispatch;
  }

  public Neo4jReplicationOptions setMaxConcurrentDispatch(int maxConcurrentDispatch) {
    this.maxConcurrentDispatch = maxConcurrentDispatch;
    return this;
  }

  @GenIgnore
  public LsnStore getLsnStore() {
    return lsnStore;
  }

  @GenIgnore
  public Neo4jReplicationOptions setLsnStore(LsnStore lsnStore) {
    this.lsnStore = Objects.requireNonNull(lsnStore, "lsnStore");
    return this;
  }

  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    Neo4jReplicationOptionsConverter.toJson(this, json);
    return json;
  }

  void validate() {
    require("uri", uri);
    require("database", database);
    require("user", user);
    require("sourceName", sourceName);
    require("eventQuery", eventQuery);
    if (pollIntervalMs < 1L) {
      throw new IllegalArgumentException("pollIntervalMs must be >= 1");
    }
    if (batchSize < 1) {
      throw new IllegalArgumentException("batchSize must be >= 1");
    }
    if (maxConcurrentDispatch < 1) {
      throw new IllegalArgumentException("maxConcurrentDispatch must be >= 1");
    }
    Objects.requireNonNull(retryPolicy, "retryPolicy").validate();
    Objects.requireNonNull(lsnStore, "lsnStore");
  }

  private static void require(String fieldName, String value) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(fieldName + " is required");
    }
  }

  private void init() {
    uri = DEFAULT_URI;
    sourceName = "neo4j";
    eventQuery = DEFAULT_QUERY;
    pollIntervalMs = 500L;
    batchSize = 500;
    retryPolicy = RetryPolicy.exponentialBackoff();
    preflightEnabled = true;
    autoStart = true;
    maxConcurrentDispatch = 1;
    lsnStore = new NoopLsnStore();
  }
}
