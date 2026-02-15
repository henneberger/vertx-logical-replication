package dev.henneberger.vertx.mongodb.replication;

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
public class MongoDbReplicationOptions {

  public static final String DEFAULT_CONNECTION_STRING = "mongodb://localhost:27017";

  private String connectionString;
  private String database;
  private String collection;
  private boolean fullDocumentLookup;
  private int batchSize;
  private RetryPolicy retryPolicy;
  private boolean preflightEnabled;
  private boolean autoStart;
  private int maxConcurrentDispatch;
  private LsnStore lsnStore;

  public MongoDbReplicationOptions() {
    init();
  }

  public MongoDbReplicationOptions(JsonObject json) {
    init();
    MongoDbReplicationOptionsConverter.fromJson(json, this);
  }

  public MongoDbReplicationOptions(MongoDbReplicationOptions other) {
    this.connectionString = other.connectionString;
    this.database = other.database;
    this.collection = other.collection;
    this.fullDocumentLookup = other.fullDocumentLookup;
    this.batchSize = other.batchSize;
    this.retryPolicy = other.retryPolicy.copy();
    this.preflightEnabled = other.preflightEnabled;
    this.autoStart = other.autoStart;
    this.maxConcurrentDispatch = other.maxConcurrentDispatch;
    this.lsnStore = other.lsnStore;
  }

  public String getConnectionString() {
    return connectionString;
  }

  public MongoDbReplicationOptions setConnectionString(String connectionString) {
    this.connectionString = connectionString;
    return this;
  }

  public String getDatabase() {
    return database;
  }

  public MongoDbReplicationOptions setDatabase(String database) {
    this.database = database;
    return this;
  }

  public String getCollection() {
    return collection;
  }

  public MongoDbReplicationOptions setCollection(String collection) {
    this.collection = collection;
    return this;
  }

  public boolean isFullDocumentLookup() {
    return fullDocumentLookup;
  }

  public MongoDbReplicationOptions setFullDocumentLookup(boolean fullDocumentLookup) {
    this.fullDocumentLookup = fullDocumentLookup;
    return this;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public MongoDbReplicationOptions setBatchSize(int batchSize) {
    this.batchSize = batchSize;
    return this;
  }

  public RetryPolicy getRetryPolicy() {
    return retryPolicy;
  }

  @GenIgnore
  public MongoDbReplicationOptions setRetryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
    return this;
  }

  public boolean isPreflightEnabled() {
    return preflightEnabled;
  }

  public MongoDbReplicationOptions setPreflightEnabled(boolean preflightEnabled) {
    this.preflightEnabled = preflightEnabled;
    return this;
  }

  public boolean isAutoStart() {
    return autoStart;
  }

  public MongoDbReplicationOptions setAutoStart(boolean autoStart) {
    this.autoStart = autoStart;
    return this;
  }

  public int getMaxConcurrentDispatch() {
    return maxConcurrentDispatch;
  }

  public MongoDbReplicationOptions setMaxConcurrentDispatch(int maxConcurrentDispatch) {
    this.maxConcurrentDispatch = maxConcurrentDispatch;
    return this;
  }

  @GenIgnore
  public LsnStore getLsnStore() {
    return lsnStore;
  }

  @GenIgnore
  public MongoDbReplicationOptions setLsnStore(LsnStore lsnStore) {
    this.lsnStore = Objects.requireNonNull(lsnStore, "lsnStore");
    return this;
  }

  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    MongoDbReplicationOptionsConverter.toJson(this, json);
    return json;
  }

  void validate() {
    require("connectionString", connectionString);
    require("database", database);
    require("collection", collection);
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
    connectionString = DEFAULT_CONNECTION_STRING;
    fullDocumentLookup = true;
    batchSize = 256;
    retryPolicy = RetryPolicy.exponentialBackoff();
    preflightEnabled = true;
    autoStart = true;
    maxConcurrentDispatch = 1;
    lsnStore = new NoopLsnStore();
  }
}
