package dev.henneberger.vertx.mysql.replication;

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
public class MySqlReplicationOptions {

  public static final String DEFAULT_HOST = "localhost";
  public static final int DEFAULT_PORT = 3306;

  private String host;
  private int port;
  private String database;
  private String user;
  private String password;
  private String passwordEnv;
  private long serverId;
  private long connectTimeoutMs;
  private RetryPolicy retryPolicy;
  private boolean preflightEnabled;
  private boolean autoStart;
  private int maxConcurrentDispatch;
  private LsnStore lsnStore;

  public MySqlReplicationOptions() {
    init();
  }

  public MySqlReplicationOptions(JsonObject json) {
    init();
    MySqlReplicationOptionsConverter.fromJson(json, this);
  }

  public MySqlReplicationOptions(MySqlReplicationOptions other) {
    this.host = other.host;
    this.port = other.port;
    this.database = other.database;
    this.user = other.user;
    this.password = other.password;
    this.passwordEnv = other.passwordEnv;
    this.serverId = other.serverId;
    this.connectTimeoutMs = other.connectTimeoutMs;
    this.retryPolicy = other.retryPolicy.copy();
    this.preflightEnabled = other.preflightEnabled;
    this.autoStart = other.autoStart;
    this.maxConcurrentDispatch = other.maxConcurrentDispatch;
    this.lsnStore = other.lsnStore;
  }

  public String getHost() { return host; }
  public MySqlReplicationOptions setHost(String host) { this.host = host; return this; }

  public Integer getPort() { return port; }
  public MySqlReplicationOptions setPort(Integer port) { this.port = port; return this; }

  public String getDatabase() { return database; }
  public MySqlReplicationOptions setDatabase(String database) { this.database = database; return this; }

  public String getUser() { return user; }
  public MySqlReplicationOptions setUser(String user) { this.user = user; return this; }

  public String getPassword() { return password; }
  public MySqlReplicationOptions setPassword(String password) { this.password = password; return this; }

  public String getPasswordEnv() { return passwordEnv; }
  public MySqlReplicationOptions setPasswordEnv(String passwordEnv) { this.passwordEnv = passwordEnv; return this; }

  public long getServerId() { return serverId; }
  public MySqlReplicationOptions setServerId(long serverId) { this.serverId = serverId; return this; }

  public long getConnectTimeoutMs() { return connectTimeoutMs; }
  public MySqlReplicationOptions setConnectTimeoutMs(long connectTimeoutMs) { this.connectTimeoutMs = connectTimeoutMs; return this; }

  public RetryPolicy getRetryPolicy() { return retryPolicy; }
  @GenIgnore
  public MySqlReplicationOptions setRetryPolicy(RetryPolicy retryPolicy) { this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy"); return this; }

  public boolean isPreflightEnabled() { return preflightEnabled; }
  public MySqlReplicationOptions setPreflightEnabled(boolean preflightEnabled) { this.preflightEnabled = preflightEnabled; return this; }

  public boolean isAutoStart() { return autoStart; }
  public MySqlReplicationOptions setAutoStart(boolean autoStart) { this.autoStart = autoStart; return this; }

  public int getMaxConcurrentDispatch() { return maxConcurrentDispatch; }
  public MySqlReplicationOptions setMaxConcurrentDispatch(int maxConcurrentDispatch) { this.maxConcurrentDispatch = maxConcurrentDispatch; return this; }

  @GenIgnore
  public LsnStore getLsnStore() { return lsnStore; }
  @GenIgnore
  public MySqlReplicationOptions setLsnStore(LsnStore lsnStore) { this.lsnStore = Objects.requireNonNull(lsnStore, "lsnStore"); return this; }

  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    MySqlReplicationOptionsConverter.toJson(this, json);
    return json;
  }

  void validate() {
    OptionValidation.require("host", host);
    OptionValidation.requirePort(port);
    OptionValidation.require("database", database);
    OptionValidation.require("user", user);
    OptionValidation.requireMin("serverId", serverId, 1);
    OptionValidation.requireMin("connectTimeoutMs", connectTimeoutMs, 1);
    OptionValidation.requireMin("maxConcurrentDispatch", maxConcurrentDispatch, 1);
    Objects.requireNonNull(retryPolicy, "retryPolicy").validate();
    Objects.requireNonNull(lsnStore, "lsnStore");
  }

  private void init() {
    host = DEFAULT_HOST;
    port = DEFAULT_PORT;
    serverId = 1001L;
    connectTimeoutMs = 10000L;
    retryPolicy = RetryPolicy.exponentialBackoff();
    preflightEnabled = true;
    autoStart = true;
    maxConcurrentDispatch = 1;
    lsnStore = new NoopLsnStore();
  }
}
