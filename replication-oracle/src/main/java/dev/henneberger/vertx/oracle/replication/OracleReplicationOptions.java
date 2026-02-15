package dev.henneberger.vertx.oracle.replication;

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
public class OracleReplicationOptions {

  public static final String DEFAULT_HOST = "localhost";
  public static final int DEFAULT_PORT = 1521;

  private String host;
  private int port;
  private String database;
  private String user;
  private String password;
  private String passwordEnv;
  private String sourceTable;
  private String positionColumn;
  private String operationColumn;
  private String beforeColumn;
  private String afterColumn;
  private String commitTimestampColumn;
  private long pollIntervalMs;
  private int batchSize;
  private RetryPolicy retryPolicy;
  private boolean preflightEnabled;
  private boolean autoStart;
  private int maxConcurrentDispatch;
  private LsnStore lsnStore;

  public OracleReplicationOptions() { init(); }
  public OracleReplicationOptions(JsonObject json) { init(); OracleReplicationOptionsConverter.fromJson(json, this); }

  public OracleReplicationOptions(OracleReplicationOptions other) {
    this.host = other.host;
    this.port = other.port;
    this.database = other.database;
    this.user = other.user;
    this.password = other.password;
    this.passwordEnv = other.passwordEnv;
    this.sourceTable = other.sourceTable;
    this.positionColumn = other.positionColumn;
    this.operationColumn = other.operationColumn;
    this.beforeColumn = other.beforeColumn;
    this.afterColumn = other.afterColumn;
    this.commitTimestampColumn = other.commitTimestampColumn;
    this.pollIntervalMs = other.pollIntervalMs;
    this.batchSize = other.batchSize;
    this.retryPolicy = other.retryPolicy.copy();
    this.preflightEnabled = other.preflightEnabled;
    this.autoStart = other.autoStart;
    this.maxConcurrentDispatch = other.maxConcurrentDispatch;
    this.lsnStore = other.lsnStore;
  }

  public String getHost() { return host; }
  public OracleReplicationOptions setHost(String host) { this.host = host; return this; }
  public Integer getPort() { return port; }
  public OracleReplicationOptions setPort(Integer port) { this.port = port; return this; }
  public String getDatabase() { return database; }
  public OracleReplicationOptions setDatabase(String database) { this.database = database; return this; }
  public String getUser() { return user; }
  public OracleReplicationOptions setUser(String user) { this.user = user; return this; }
  public String getPassword() { return password; }
  public OracleReplicationOptions setPassword(String password) { this.password = password; return this; }
  public String getPasswordEnv() { return passwordEnv; }
  public OracleReplicationOptions setPasswordEnv(String passwordEnv) { this.passwordEnv = passwordEnv; return this; }
  public String getSourceTable() { return sourceTable; }
  public OracleReplicationOptions setSourceTable(String sourceTable) { this.sourceTable = sourceTable; return this; }
  public String getPositionColumn() { return positionColumn; }
  public OracleReplicationOptions setPositionColumn(String positionColumn) { this.positionColumn = positionColumn; return this; }
  public String getOperationColumn() { return operationColumn; }
  public OracleReplicationOptions setOperationColumn(String operationColumn) { this.operationColumn = operationColumn; return this; }
  public String getBeforeColumn() { return beforeColumn; }
  public OracleReplicationOptions setBeforeColumn(String beforeColumn) { this.beforeColumn = beforeColumn; return this; }
  public String getAfterColumn() { return afterColumn; }
  public OracleReplicationOptions setAfterColumn(String afterColumn) { this.afterColumn = afterColumn; return this; }
  public String getCommitTimestampColumn() { return commitTimestampColumn; }
  public OracleReplicationOptions setCommitTimestampColumn(String commitTimestampColumn) { this.commitTimestampColumn = commitTimestampColumn; return this; }
  public long getPollIntervalMs() { return pollIntervalMs; }
  public OracleReplicationOptions setPollIntervalMs(long pollIntervalMs) { this.pollIntervalMs = pollIntervalMs; return this; }
  public int getBatchSize() { return batchSize; }
  public OracleReplicationOptions setBatchSize(int batchSize) { this.batchSize = batchSize; return this; }
  public RetryPolicy getRetryPolicy() { return retryPolicy; }

  @GenIgnore
  public OracleReplicationOptions setRetryPolicy(RetryPolicy retryPolicy) { this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy"); return this; }

  public boolean isPreflightEnabled() { return preflightEnabled; }
  public OracleReplicationOptions setPreflightEnabled(boolean preflightEnabled) { this.preflightEnabled = preflightEnabled; return this; }
  public boolean isAutoStart() { return autoStart; }
  public OracleReplicationOptions setAutoStart(boolean autoStart) { this.autoStart = autoStart; return this; }
  public int getMaxConcurrentDispatch() { return maxConcurrentDispatch; }
  public OracleReplicationOptions setMaxConcurrentDispatch(int maxConcurrentDispatch) { this.maxConcurrentDispatch = maxConcurrentDispatch; return this; }

  @GenIgnore
  public LsnStore getLsnStore() { return lsnStore; }
  @GenIgnore
  public OracleReplicationOptions setLsnStore(LsnStore lsnStore) { this.lsnStore = Objects.requireNonNull(lsnStore, "lsnStore"); return this; }

  public JsonObject toJson() { JsonObject json = new JsonObject(); OracleReplicationOptionsConverter.toJson(this, json); return json; }

  void validate() {
    OptionValidation.require("host", host);
    OptionValidation.requirePort(port);
    OptionValidation.require("database", database);
    OptionValidation.require("user", user);
    OptionValidation.require("sourceTable", sourceTable);
    OptionValidation.require("positionColumn", positionColumn);
    OptionValidation.requireMin("pollIntervalMs", pollIntervalMs, 1);
    OptionValidation.requireMin("batchSize", batchSize, 1);
    OptionValidation.requireMin("maxConcurrentDispatch", maxConcurrentDispatch, 1);
    Objects.requireNonNull(retryPolicy, "retryPolicy").validate();
    Objects.requireNonNull(lsnStore, "lsnStore");
  }

  private void init() {
    host = DEFAULT_HOST;
    port = DEFAULT_PORT;
    positionColumn = "position";
    operationColumn = "operation";
    beforeColumn = "before_json";
    afterColumn = "after_json";
    commitTimestampColumn = "commit_ts";
    pollIntervalMs = 500L;
    batchSize = 500;
    retryPolicy = RetryPolicy.exponentialBackoff();
    preflightEnabled = true;
    autoStart = true;
    maxConcurrentDispatch = 1;
    lsnStore = new NoopLsnStore();
  }
}
