package dev.henneberger.vertx.mariadb.replication;

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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MariaDbLogicalReplicationStream extends AbstractJdbcPollingReplicationStream<MariaDbChangeEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(MariaDbLogicalReplicationStream.class);

  private final MariaDbReplicationOptions options;

  public MariaDbLogicalReplicationStream(Vertx vertx, MariaDbReplicationOptions options) {
    super(vertx);
    this.options = new MariaDbReplicationOptions(Objects.requireNonNull(options, "options"));
    this.options.validate();
  }

  public MariaDbChangeSubscription subscribe(MariaDbChangeFilter filter,
                                             MariaDbChangeConsumer eventConsumer,
                                             Handler<Throwable> errorHandler) {
    ReplicationSubscription subscription = registerSubscription(filter, eventConsumer, errorHandler, true);
    return subscription::cancel;
  }

  @Override
  public ReplicationSubscription subscribe(ChangeFilter<MariaDbChangeEvent> filter,
                                           ChangeConsumer<MariaDbChangeEvent> eventConsumer,
                                           Handler<Throwable> errorHandler) {
    return registerSubscription(filter, eventConsumer, errorHandler, true);
  }

  @Override
  protected String streamName() {
    return "mariadb-cdc";
  }

  @Override
  protected void logStreamFailure(Throwable error) {
    LOG.error("MariaDb CDC stream failed for {}", options.getSourceTable(), error);
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
  protected String rowLimitClause() {
    return "LIMIT ?";
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
    String url = "jdbc:mariadb://" + options.getHost() + ":" + options.getPort() + "/" + options.getDatabase();
    return DriverManager.getConnection(url, options.getUser(), resolvePassword());
  }

  @Override
  protected MariaDbChangeEvent mapRow(ResultSet rs) throws Exception {
    Map<String, Object> row = extractRow(rs);
    String operationRaw = asString(row.get(options.getOperationColumn()));
    MariaDbChangeEvent.Operation operation = mapOperation(operationRaw);
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
    metadata.put("adapter", "mariadb");
    metadata.put("rawOperation", operationRaw);
    return new MariaDbChangeEvent(options.getSourceTable(), operation, before, after, position, commitTs, metadata);
  }

  @Override
  protected String eventPosition(MariaDbChangeEvent event) {
    return event.getPosition();
  }

  @Override
  protected String checkpointKey() {
    return "mariadb:" + options.getDatabase() + ":" + options.getSourceTable();
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

  private static MariaDbChangeEvent.Operation mapOperation(String op) {
    String n = op == null ? "" : op.trim().toUpperCase(Locale.ROOT);
    if (n.startsWith("INS")) {
      return MariaDbChangeEvent.Operation.INSERT;
    }
    if (n.startsWith("DEL")) {
      return MariaDbChangeEvent.Operation.DELETE;
    }
    return MariaDbChangeEvent.Operation.UPDATE;
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

  private static Instant toInstant(Object raw) {
    if (raw instanceof Instant) {
      return (Instant) raw;
    }
    if (raw instanceof Timestamp) {
      return ((Timestamp) raw).toInstant();
    }
    if (raw instanceof Date) {
      return ((Date) raw).toInstant();
    }
    try {
      return Instant.parse(String.valueOf(raw));
    } catch (Exception ignore) {
      return null;
    }
  }

  private static Object normalizeValue(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Timestamp) {
      return ((Timestamp) value).toInstant().toString();
    }
    if (value instanceof Date) {
      return ((Date) value).toInstant().toString();
    }
    if (value instanceof LocalDateTime) {
      return ((LocalDateTime) value).toString();
    }
    if (value instanceof LocalDate) {
      return ((LocalDate) value).toString();
    }
    if (value instanceof LocalTime) {
      return ((LocalTime) value).toString();
    }
    if (value instanceof java.sql.Array) {
      try {
        Object arr = ((java.sql.Array) value).getArray();
        if (arr instanceof Object[]) {
          Object[] vals = (Object[]) arr;
          List<Object> out = new ArrayList<>(vals.length);
          for (Object item : vals) {
            out.add(normalizeValue(item));
          }
          return out;
        }
      } catch (Exception ignore) {
      }
    }
    return value;
  }

  private static String asString(Object value) {
    return value == null ? "" : String.valueOf(value);
  }

  private static Map<String, Object> extractRow(ResultSet rs) throws Exception {
    ResultSetMetaData md = rs.getMetaData();
    Map<String, Object> out = new LinkedHashMap<>();
    for (int i = 1; i <= md.getColumnCount(); i++) {
      out.put(md.getColumnLabel(i), normalizeValue(rs.getObject(i)));
    }
    return out;
  }
}
