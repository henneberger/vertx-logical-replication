package dev.henneberger.vertx.db2.replication;

import dev.henneberger.vertx.replication.core.AbstractJdbcPollingReplicationStream;
import dev.henneberger.vertx.replication.core.ChangeConsumer;
import dev.henneberger.vertx.replication.core.ChangeFilter;
import dev.henneberger.vertx.replication.core.ReplicationSubscription;
import dev.henneberger.vertx.replication.core.RetryPolicy;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Db2LogicalReplicationStream extends AbstractJdbcPollingReplicationStream<Db2ChangeEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(Db2LogicalReplicationStream.class);

  private final Db2ReplicationOptions options;

  public Db2LogicalReplicationStream(Vertx vertx, Db2ReplicationOptions options) {
    super(vertx);
    this.options = new Db2ReplicationOptions(Objects.requireNonNull(options, "options"));
    this.options.validate();
  }

  public Db2ChangeSubscription subscribe(Db2ChangeFilter filter,
                                         Db2ChangeConsumer eventConsumer,
                                         Handler<Throwable> errorHandler) {
    ReplicationSubscription subscription = registerSubscription(filter, eventConsumer, errorHandler, true);
    return subscription::cancel;
  }

  @Override
  public ReplicationSubscription subscribe(ChangeFilter<Db2ChangeEvent> filter,
                                           ChangeConsumer<Db2ChangeEvent> eventConsumer,
                                           Handler<Throwable> errorHandler) {
    return registerSubscription(filter, eventConsumer, errorHandler, true);
  }

  @Override
  protected String streamName() {
    return "db2-cdc";
  }

  @Override
  protected void logStreamFailure(Throwable error) {
    LOG.error("Db2 CDC stream failed for {}", options.getSourceTable(), error);
  }

  @Override
  protected String sourceTable() {
    return options.getSourceTable();
  }

  @Override
  protected String positionColumn() {
    return options.getPositionColumn();
  }

  @Override
  protected int batchSize() {
    return options.getBatchSize();
  }

  @Override
  protected int maxConcurrentDispatch() {
    return options.getMaxConcurrentDispatch();
  }

  @Override
  protected long pollIntervalMs() {
    return options.getPollIntervalMs();
  }

  @Override
  protected RetryPolicy retryPolicy() {
    return options.getRetryPolicy();
  }

  @Override
  protected boolean preflightEnabled() {
    return options.isPreflightEnabled();
  }

  @Override
  protected boolean autoStart() {
    return options.isAutoStart();
  }

  @Override
  protected Connection openConnection() throws Exception {
    String url = "jdbc:db2://" + options.getHost() + ":" + options.getPort() + "/" + options.getDatabase();
    return DriverManager.getConnection(url, options.getUser(), resolvePassword());
  }

  @Override
  protected Db2ChangeEvent mapRow(ResultSet rs) throws Exception {
    Map<String, Object> row = extractRow(rs);
    String operationRaw = asString(row.get(options.getOperationColumn()));
    Db2ChangeEvent.Operation operation = mapOperation(operationRaw);
    Map<String, Object> before = parseMap(row.get(options.getBeforeColumn()));
    Map<String, Object> after = parseMap(row.get(options.getAfterColumn()));
    if (after.isEmpty()) {
      after = new LinkedHashMap<>(row);
      after.remove(options.getOperationColumn());
      after.remove(options.getBeforeColumn());
      after.remove(options.getAfterColumn());
    }
    Instant commitTs = toInstant(row.get(options.getCommitTimestampColumn()));
    String position = asString(row.get(options.getPositionColumn()));
    Map<String, Object> metadata = new LinkedHashMap<>();
    metadata.put("adapter", "db2");
    metadata.put("rawOperation", operationRaw);
    return new Db2ChangeEvent(options.getSourceTable(), operation, before, after, position, commitTs, metadata);
  }

  @Override
  protected String eventPosition(Db2ChangeEvent event) {
    return event.getPosition();
  }

  @Override
  protected String checkpointKey() {
    return "db2:" + options.getDatabase() + ":" + options.getSourceTable();
  }

  @Override
  protected Optional<String> loadCheckpoint() throws Exception {
    return options.getLsnStore().load(checkpointKey());
  }

  @Override
  protected void saveCheckpoint(String token) throws Exception {
    options.getLsnStore().save(checkpointKey(), token);
  }

  private String resolvePassword() {
    String password = options.getPassword();
    if (password == null || password.isBlank()) {
      String env = options.getPasswordEnv();
      if (env != null && !env.isBlank()) {
        password = System.getenv(env);
      }
    }
    return password == null ? "" : password;
  }

  private static Db2ChangeEvent.Operation mapOperation(String op) {
    String n = op == null ? "" : op.trim().toUpperCase(Locale.ROOT);
    if (n.startsWith("INS")) {
      return Db2ChangeEvent.Operation.INSERT;
    }
    if (n.startsWith("DEL")) {
      return Db2ChangeEvent.Operation.DELETE;
    }
    return Db2ChangeEvent.Operation.UPDATE;
  }

  private static Map<String, Object> parseMap(Object raw) {
    if (raw == null) {
      return Collections.emptyMap();
    }
    if (raw instanceof Map) {
      return new LinkedHashMap<>((Map<String, Object>) raw);
    }
    try {
      JsonObject json = raw instanceof JsonObject ? (JsonObject) raw : new JsonObject(String.valueOf(raw));
      return json.getMap();
    } catch (Exception ignore) {
      return Collections.emptyMap();
    }
  }

  private static Instant toInstant(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Instant) {
      return (Instant) value;
    }
    if (value instanceof Timestamp) {
      return ((Timestamp) value).toInstant();
    }
    try {
      return Instant.parse(String.valueOf(value));
    } catch (Exception ignore) {
      return null;
    }
  }

  private static String asString(Object value) {
    return value == null ? "" : String.valueOf(value);
  }

  private static Map<String, Object> extractRow(ResultSet rs) throws Exception {
    ResultSetMetaData md = rs.getMetaData();
    Map<String, Object> out = new LinkedHashMap<>();
    for (int i = 1; i <= md.getColumnCount(); i++) {
      out.put(md.getColumnLabel(i), rs.getObject(i));
    }
    return out;
  }
}
