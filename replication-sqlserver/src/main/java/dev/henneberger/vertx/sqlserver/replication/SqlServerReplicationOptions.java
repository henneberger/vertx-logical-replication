package dev.henneberger.vertx.sqlserver.replication;

import dev.henneberger.vertx.replication.core.LsnStore;
import dev.henneberger.vertx.replication.core.NoopLsnStore;
import dev.henneberger.vertx.replication.core.OptionValidation;
import dev.henneberger.vertx.replication.core.RetryPolicy;
import dev.henneberger.vertx.replication.core.ValueNormalizationMode;
import dev.henneberger.vertx.replication.core.ValueNormalizer;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.json.annotations.JsonGen;
import io.vertx.core.json.JsonObject;
import java.util.Locale;
import java.util.Objects;

@DataObject
@JsonGen(publicConverter = false)
public class SqlServerReplicationOptions {

  public static final String DEFAULT_HOST = "localhost";
  public static final int DEFAULT_PORT = 1433;

  private String host;
  private int port;
  private String database;
  private String user;
  private String password;
  private String passwordEnv;
  private boolean ssl;
  private String captureInstance;
  private long pollIntervalMs;
  private int maxBatchSize;
  private RetryPolicy retryPolicy;
  private boolean preflightEnabled;
  private String preflightMode;
  private long preflightMaxWaitMs;
  private long preflightRetryIntervalMs;
  private boolean autoStart;
  private int maxConcurrentDispatch;
  private ValueNormalizationMode valueNormalizationMode;
  private ValueNormalizer valueNormalizer;
  private LsnStore lsnStore;

  public SqlServerReplicationOptions() {
    init();
  }

  public SqlServerReplicationOptions(JsonObject json) {
    init();
    SqlServerReplicationOptionsConverter.fromJson(json, this);
  }

  public SqlServerReplicationOptions(SqlServerReplicationOptions other) {
    this.host = other.host;
    this.port = other.port;
    this.database = other.database;
    this.user = other.user;
    this.password = other.password;
    this.passwordEnv = other.passwordEnv;
    this.ssl = other.ssl;
    this.captureInstance = other.captureInstance;
    this.pollIntervalMs = other.pollIntervalMs;
    this.maxBatchSize = other.maxBatchSize;
    this.retryPolicy = other.retryPolicy.copy();
    this.preflightEnabled = other.preflightEnabled;
    this.preflightMode = other.preflightMode;
    this.preflightMaxWaitMs = other.preflightMaxWaitMs;
    this.preflightRetryIntervalMs = other.preflightRetryIntervalMs;
    this.autoStart = other.autoStart;
    this.maxConcurrentDispatch = other.maxConcurrentDispatch;
    this.valueNormalizationMode = other.valueNormalizationMode;
    this.valueNormalizer = other.valueNormalizer;
    this.lsnStore = other.lsnStore;
  }

  public String getHost() {
    return host;
  }

  public SqlServerReplicationOptions setHost(String host) {
    this.host = host;
    return this;
  }

  public Integer getPort() {
    return port;
  }

  public SqlServerReplicationOptions setPort(Integer port) {
    this.port = port;
    return this;
  }

  public String getDatabase() {
    return database;
  }

  public SqlServerReplicationOptions setDatabase(String database) {
    this.database = database;
    return this;
  }

  public String getUser() {
    return user;
  }

  public SqlServerReplicationOptions setUser(String user) {
    this.user = user;
    return this;
  }

  public String getPassword() {
    return password;
  }

  public SqlServerReplicationOptions setPassword(String password) {
    this.password = password;
    return this;
  }

  public String getPasswordEnv() {
    return passwordEnv;
  }

  public SqlServerReplicationOptions setPasswordEnv(String passwordEnv) {
    this.passwordEnv = passwordEnv;
    return this;
  }

  public Boolean getSsl() {
    return ssl;
  }

  public SqlServerReplicationOptions setSsl(Boolean ssl) {
    this.ssl = Boolean.TRUE.equals(ssl);
    return this;
  }

  public String getCaptureInstance() {
    return captureInstance;
  }

  public SqlServerReplicationOptions setCaptureInstance(String captureInstance) {
    this.captureInstance = captureInstance;
    return this;
  }

  public long getPollIntervalMs() {
    return pollIntervalMs;
  }

  public SqlServerReplicationOptions setPollIntervalMs(long pollIntervalMs) {
    this.pollIntervalMs = pollIntervalMs;
    return this;
  }

  public int getMaxBatchSize() {
    return maxBatchSize;
  }

  public SqlServerReplicationOptions setMaxBatchSize(int maxBatchSize) {
    this.maxBatchSize = maxBatchSize;
    return this;
  }

  public RetryPolicy getRetryPolicy() {
    return retryPolicy;
  }

  @GenIgnore
  public SqlServerReplicationOptions setRetryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
    return this;
  }

  public boolean isPreflightEnabled() {
    return preflightEnabled;
  }

  public SqlServerReplicationOptions setPreflightEnabled(boolean preflightEnabled) {
    this.preflightEnabled = preflightEnabled;
    return this;
  }

  public String getPreflightMode() {
    return preflightMode;
  }

  public SqlServerReplicationOptions setPreflightMode(String preflightMode) {
    this.preflightMode = preflightMode == null ? null : preflightMode.toLowerCase(Locale.ROOT);
    return this;
  }

  public long getPreflightMaxWaitMs() {
    return preflightMaxWaitMs;
  }

  public SqlServerReplicationOptions setPreflightMaxWaitMs(long preflightMaxWaitMs) {
    this.preflightMaxWaitMs = preflightMaxWaitMs;
    return this;
  }

  public long getPreflightRetryIntervalMs() {
    return preflightRetryIntervalMs;
  }

  public SqlServerReplicationOptions setPreflightRetryIntervalMs(long preflightRetryIntervalMs) {
    this.preflightRetryIntervalMs = preflightRetryIntervalMs;
    return this;
  }

  public boolean isAutoStart() {
    return autoStart;
  }

  public SqlServerReplicationOptions setAutoStart(boolean autoStart) {
    this.autoStart = autoStart;
    return this;
  }

  public int getMaxConcurrentDispatch() {
    return maxConcurrentDispatch;
  }

  public SqlServerReplicationOptions setMaxConcurrentDispatch(int maxConcurrentDispatch) {
    this.maxConcurrentDispatch = maxConcurrentDispatch;
    return this;
  }

  public String getValueNormalizationMode() {
    return valueNormalizationMode.name();
  }

  public SqlServerReplicationOptions setValueNormalizationMode(String valueNormalizationMode) {
    this.valueNormalizationMode = ValueNormalizationMode.valueOf(valueNormalizationMode.toUpperCase(Locale.ROOT));
    return this;
  }

  @GenIgnore
  public ValueNormalizationMode valueNormalizationMode() {
    return valueNormalizationMode;
  }

  @GenIgnore
  public ValueNormalizer getValueNormalizer() {
    return valueNormalizer;
  }

  @GenIgnore
  public SqlServerReplicationOptions setValueNormalizer(ValueNormalizer valueNormalizer) {
    this.valueNormalizer = valueNormalizer;
    return this;
  }

  @GenIgnore
  public LsnStore getLsnStore() {
    return lsnStore;
  }

  @GenIgnore
  public SqlServerReplicationOptions setLsnStore(LsnStore lsnStore) {
    this.lsnStore = Objects.requireNonNull(lsnStore, "lsnStore");
    return this;
  }

  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    SqlServerReplicationOptionsConverter.toJson(this, json);
    return json;
  }

  public SqlServerReplicationOptions merge(JsonObject other) {
    JsonObject json = toJson();
    json.mergeIn(other);
    return new SqlServerReplicationOptions(json);
  }

  void validate() {
    OptionValidation.require("host", host);
    OptionValidation.requirePort(port);
    OptionValidation.require("database", database);
    OptionValidation.require("user", user);
    OptionValidation.require("captureInstance", captureInstance);
    OptionValidation.requireMin("pollIntervalMs", pollIntervalMs, 1);
    OptionValidation.requireMin("maxBatchSize", maxBatchSize, 1);
    if (!"strict".equals(preflightMode) && !"wait-until-ready".equals(preflightMode)) {
      throw new IllegalArgumentException("preflightMode must be 'strict' or 'wait-until-ready'");
    }
    OptionValidation.requireMin("preflightMaxWaitMs", preflightMaxWaitMs, 0);
    OptionValidation.requireMin("preflightRetryIntervalMs", preflightRetryIntervalMs, 1);
    OptionValidation.requireMin("maxConcurrentDispatch", maxConcurrentDispatch, 1);
    Objects.requireNonNull(retryPolicy, "retryPolicy").validate();
    Objects.requireNonNull(lsnStore, "lsnStore");
  }

  private void init() {
    host = DEFAULT_HOST;
    port = DEFAULT_PORT;
    ssl = true;
    pollIntervalMs = 500;
    maxBatchSize = 1000;
    retryPolicy = RetryPolicy.exponentialBackoff();
    preflightEnabled = true;
    preflightMode = "strict";
    preflightMaxWaitMs = 30000L;
    preflightRetryIntervalMs = 500L;
    autoStart = true;
    maxConcurrentDispatch = 1;
    valueNormalizationMode = ValueNormalizationMode.JSON_SAFE;
    valueNormalizer = null;
    lsnStore = new NoopLsnStore();
  }
}
