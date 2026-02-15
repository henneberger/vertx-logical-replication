package dev.henneberger.vertx.scylladb.replication;

import dev.henneberger.vertx.replication.core.LsnStore;
import dev.henneberger.vertx.replication.core.NoopLsnStore;
import dev.henneberger.vertx.replication.core.OptionValidation;
import dev.henneberger.vertx.replication.core.RetryPolicy;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.json.annotations.JsonGen;
import io.vertx.core.json.JsonObject;
import java.util.Objects;

@DataObject
@JsonGen(publicConverter = false)
public class ScyllaDbReplicationOptions {

  public static final String DEFAULT_HOST = "localhost";
  public static final int DEFAULT_PORT = 9042;

  private String host;
  private int port;
  private String localDatacenter;
  private String keyspace;
  private String sourceTable;
  private String user;
  private String password;
  private String passwordEnv;
  private String positionColumn;
  private String operationColumn;
  private long pollIntervalMs;
  private int batchSize;
  private RetryPolicy retryPolicy;
  private boolean preflightEnabled;
  private boolean autoStart;
  private int maxConcurrentDispatch;
  private LsnStore lsnStore;

  public ScyllaDbReplicationOptions() {
    init();
  }

  public ScyllaDbReplicationOptions(JsonObject json) {
    init();
    ScyllaDbReplicationOptionsConverter.fromJson(json, this);
  }

  public ScyllaDbReplicationOptions(ScyllaDbReplicationOptions other) {
    this.host = other.host;
    this.port = other.port;
    this.localDatacenter = other.localDatacenter;
    this.keyspace = other.keyspace;
    this.sourceTable = other.sourceTable;
    this.user = other.user;
    this.password = other.password;
    this.passwordEnv = other.passwordEnv;
    this.positionColumn = other.positionColumn;
    this.operationColumn = other.operationColumn;
    this.pollIntervalMs = other.pollIntervalMs;
    this.batchSize = other.batchSize;
    this.retryPolicy = other.retryPolicy.copy();
    this.preflightEnabled = other.preflightEnabled;
    this.autoStart = other.autoStart;
    this.maxConcurrentDispatch = other.maxConcurrentDispatch;
    this.lsnStore = other.lsnStore;
  }

  public String getHost() {
    return host;
  }

  public ScyllaDbReplicationOptions setHost(String host) {
    this.host = host;
    return this;
  }

  public int getPort() {
    return port;
  }

  public ScyllaDbReplicationOptions setPort(int port) {
    this.port = port;
    return this;
  }

  public String getLocalDatacenter() {
    return localDatacenter;
  }

  public ScyllaDbReplicationOptions setLocalDatacenter(String localDatacenter) {
    this.localDatacenter = localDatacenter;
    return this;
  }

  public String getKeyspace() {
    return keyspace;
  }

  public ScyllaDbReplicationOptions setKeyspace(String keyspace) {
    this.keyspace = keyspace;
    return this;
  }

  public String getSourceTable() {
    return sourceTable;
  }

  public ScyllaDbReplicationOptions setSourceTable(String sourceTable) {
    this.sourceTable = sourceTable;
    return this;
  }

  public String getUser() {
    return user;
  }

  public ScyllaDbReplicationOptions setUser(String user) {
    this.user = user;
    return this;
  }

  public String getPassword() {
    return password;
  }

  public ScyllaDbReplicationOptions setPassword(String password) {
    this.password = password;
    return this;
  }

  public String getPasswordEnv() {
    return passwordEnv;
  }

  public ScyllaDbReplicationOptions setPasswordEnv(String passwordEnv) {
    this.passwordEnv = passwordEnv;
    return this;
  }

  public String getPositionColumn() {
    return positionColumn;
  }

  public ScyllaDbReplicationOptions setPositionColumn(String positionColumn) {
    this.positionColumn = positionColumn;
    return this;
  }

  public String getOperationColumn() {
    return operationColumn;
  }

  public ScyllaDbReplicationOptions setOperationColumn(String operationColumn) {
    this.operationColumn = operationColumn;
    return this;
  }

  public long getPollIntervalMs() {
    return pollIntervalMs;
  }

  public ScyllaDbReplicationOptions setPollIntervalMs(long pollIntervalMs) {
    this.pollIntervalMs = pollIntervalMs;
    return this;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public ScyllaDbReplicationOptions setBatchSize(int batchSize) {
    this.batchSize = batchSize;
    return this;
  }

  public RetryPolicy getRetryPolicy() {
    return retryPolicy;
  }

  @GenIgnore
  public ScyllaDbReplicationOptions setRetryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
    return this;
  }

  public boolean isPreflightEnabled() {
    return preflightEnabled;
  }

  public ScyllaDbReplicationOptions setPreflightEnabled(boolean preflightEnabled) {
    this.preflightEnabled = preflightEnabled;
    return this;
  }

  public boolean isAutoStart() {
    return autoStart;
  }

  public ScyllaDbReplicationOptions setAutoStart(boolean autoStart) {
    this.autoStart = autoStart;
    return this;
  }

  public int getMaxConcurrentDispatch() {
    return maxConcurrentDispatch;
  }

  public ScyllaDbReplicationOptions setMaxConcurrentDispatch(int maxConcurrentDispatch) {
    this.maxConcurrentDispatch = maxConcurrentDispatch;
    return this;
  }

  @GenIgnore
  public LsnStore getLsnStore() {
    return lsnStore;
  }

  @GenIgnore
  public ScyllaDbReplicationOptions setLsnStore(LsnStore lsnStore) {
    this.lsnStore = Objects.requireNonNull(lsnStore, "lsnStore");
    return this;
  }

  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    ScyllaDbReplicationOptionsConverter.toJson(this, json);
    return json;
  }

  void validate() {
    OptionValidation.require("host", host);
    OptionValidation.require("localDatacenter", localDatacenter);
    OptionValidation.require("keyspace", keyspace);
    OptionValidation.require("sourceTable", sourceTable);
    OptionValidation.require("positionColumn", positionColumn);
    OptionValidation.requirePort(port);
    OptionValidation.requireMin("pollIntervalMs", pollIntervalMs, 1);
    OptionValidation.requireMin("batchSize", batchSize, 1);
    OptionValidation.requireMin("maxConcurrentDispatch", maxConcurrentDispatch, 1);
    Objects.requireNonNull(retryPolicy, "retryPolicy").validate();
    Objects.requireNonNull(lsnStore, "lsnStore");
  }

  private void init() {
    host = DEFAULT_HOST;
    port = DEFAULT_PORT;
    localDatacenter = "datacenter1";
    positionColumn = "position";
    operationColumn = "operation";
    pollIntervalMs = 500L;
    batchSize = 500;
    retryPolicy = RetryPolicy.exponentialBackoff();
    preflightEnabled = true;
    autoStart = true;
    maxConcurrentDispatch = 1;
    lsnStore = new NoopLsnStore();
  }
}
