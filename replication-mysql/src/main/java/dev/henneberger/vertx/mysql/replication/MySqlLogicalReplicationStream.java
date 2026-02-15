package dev.henneberger.vertx.mysql.replication;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import dev.henneberger.vertx.replication.core.AbstractWorkerReplicationStream;
import dev.henneberger.vertx.replication.core.AdapterMode;
import dev.henneberger.vertx.replication.core.ChangeConsumer;
import dev.henneberger.vertx.replication.core.ChangeFilter;
import dev.henneberger.vertx.replication.core.LsnStore;
import dev.henneberger.vertx.replication.core.PreflightIssue;
import dev.henneberger.vertx.replication.core.PreflightReport;
import dev.henneberger.vertx.replication.core.RetryPolicy;
import dev.henneberger.vertx.replication.core.ReplicationStreamState;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySqlLogicalReplicationStream extends AbstractWorkerReplicationStream<MySqlChangeEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(MySqlLogicalReplicationStream.class);

  private final MySqlReplicationOptions options;
  private final Map<Long, String> tableNames = new HashMap<>();

  private volatile BinaryLogClient client;

  public MySqlLogicalReplicationStream(Vertx vertx, MySqlReplicationOptions options) {
    super(vertx);
    this.options = new MySqlReplicationOptions(Objects.requireNonNull(options, "options"));
    this.options.validate();
  }

  @Override
  public MySqlChangeSubscription subscribe(ChangeFilter<MySqlChangeEvent> filter,
                                           ChangeConsumer<MySqlChangeEvent> eventConsumer,
                                           Handler<Throwable> errorHandler) {
    return () -> registerSubscription(filter, eventConsumer, errorHandler, true).cancel();
  }

  @Override
  public AdapterMode adapterMode() {
    return AdapterMode.LOG_STREAM;
  }

  @Override
  protected String streamName() {
    return "mysql-cdc-" + options.getDatabase();
  }

  @Override
  protected int maxConcurrentDispatch() {
    return options.getMaxConcurrentDispatch();
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
  protected RetryPolicy retryPolicy() {
    return options.getRetryPolicy();
  }

  @Override
  protected LsnStore checkpointStore() {
    return options.getLsnStore();
  }

  @Override
  protected PreflightReport runPreflightChecks() {
    List<PreflightIssue> issues = new ArrayList<>();
    try (Connection conn = openConnection()) {
      String logBin = readVariable(conn, "log_bin");
      if (!"ON".equalsIgnoreCase(logBin)) {
        issues.add(new PreflightIssue(
          PreflightIssue.Severity.ERROR,
          "LOG_BIN_DISABLED",
          "log_bin is '" + logBin + "'",
          "Enable binary logging (log_bin=ON)."
        ));
      }
      String format = readVariable(conn, "binlog_format");
      if (!"ROW".equalsIgnoreCase(format)) {
        issues.add(new PreflightIssue(
          PreflightIssue.Severity.WARNING,
          "BINLOG_FORMAT_NOT_ROW",
          "binlog_format is '" + format + "'",
          "Use binlog_format=ROW for deterministic CDC."
        ));
      }
    } catch (Exception e) {
      issues.add(new PreflightIssue(
        PreflightIssue.Severity.ERROR,
        "CONNECTION_FAILED",
        "Could not connect to MySQL: " + e.getMessage(),
        "Verify host, port, database, user, password, and replication permissions."
      ));
    }
    return new PreflightReport(issues);
  }

  @Override
  protected void runSession(long attempt) throws Exception {
    BinaryLogClient localClient = new BinaryLogClient(options.getHost(), options.getPort(), options.getUser(), resolvePassword());
    localClient.setServerId(options.getServerId());
    localClient.setKeepAlive(true);
    localClient.setConnectTimeout((int) options.getConnectTimeoutMs());

    String checkpoint = loadCheckpoint(checkpointKey());
    if (!checkpoint.isBlank()) {
      String[] parsed = parseCheckpoint(checkpoint);
      localClient.setBinlogFilename(parsed[0]);
      localClient.setBinlogPosition(Long.parseLong(parsed[1]));
    }

    localClient.registerEventListener(event -> handleEvent(localClient, event));

    client = localClient;
    transition(ReplicationStreamState.RUNNING, null, attempt);
    completeStart();

    try {
      localClient.connect();
    } finally {
      client = null;
    }
  }

  @Override
  protected void logStreamFailure(Throwable error) {
    LOG.error("MySQL CDC stream failed for database {}", options.getDatabase(), error);
  }

  @Override
  protected void onCloseResources() {
    BinaryLogClient current = client;
    client = null;
    if (current != null) {
      try {
        current.disconnect();
      } catch (Exception ignore) {
      }
    }
  }

  private void handleEvent(BinaryLogClient localClient, Event event) {
    if (!shouldRun()) {
      return;
    }
    EventData data = event.getData();
    try {
      if (data instanceof TableMapEventData) {
        TableMapEventData t = (TableMapEventData) data;
        tableNames.put(t.getTableId(), t.getDatabase() + "." + t.getTable());
        return;
      }

      EventHeaderV4 header = (EventHeaderV4) event.getHeader();
      if (data instanceof WriteRowsEventData) {
        WriteRowsEventData wr = (WriteRowsEventData) data;
        String table = tableName(wr.getTableId());
        for (Serializable[] row : wr.getRows()) {
          emitChange(new MySqlChangeEvent(table, MySqlChangeEvent.Operation.INSERT,
            Collections.emptyMap(), rowMap(row), localClient.getBinlogFilename(), header.getNextPosition()));
        }
      } else if (data instanceof UpdateRowsEventData) {
        UpdateRowsEventData ur = (UpdateRowsEventData) data;
        String table = tableName(ur.getTableId());
        for (Map.Entry<Serializable[], Serializable[]> entry : ur.getRows()) {
          emitChange(new MySqlChangeEvent(table, MySqlChangeEvent.Operation.UPDATE,
            rowMap(entry.getKey()), rowMap(entry.getValue()), localClient.getBinlogFilename(), header.getNextPosition()));
        }
      } else if (data instanceof DeleteRowsEventData) {
        DeleteRowsEventData dr = (DeleteRowsEventData) data;
        String table = tableName(dr.getTableId());
        for (Serializable[] row : dr.getRows()) {
          emitChange(new MySqlChangeEvent(table, MySqlChangeEvent.Operation.DELETE,
            rowMap(row), Collections.emptyMap(), localClient.getBinlogFilename(), header.getNextPosition()));
        }
      }
    } catch (Exception e) {
      emitParseFailure("binlog-event", e);
      notifyError(e);
      try {
        localClient.disconnect();
      } catch (Exception ignore) {
      }
    }
  }

  private void emitChange(MySqlChangeEvent event) throws Exception {
    dispatchAndAwait(event);
    emitEventMetric(event);

    String token = formatCheckpoint(event.getBinlogFile(), event.getBinlogPosition());
    saveCheckpoint(checkpointKey(), token);
    emitLsnCommitted(checkpointKey(), token);
  }

  private String tableName(long tableId) {
    return tableNames.getOrDefault(tableId, "unknown.unknown");
  }

  private Map<String, Object> rowMap(Serializable[] row) {
    Map<String, Object> out = new LinkedHashMap<>();
    for (int i = 0; i < row.length; i++) {
      out.put("c" + i, normalizeValue(row[i]));
    }
    return out;
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
    if (value instanceof byte[]) {
      return new String((byte[]) value);
    }
    return value;
  }

  private String checkpointKey() {
    return "mysql:" + options.getDatabase();
  }

  static String formatCheckpoint(String file, long pos) {
    if (file == null || file.isBlank()) {
      return "";
    }
    return file + ':' + pos;
  }

  static String[] parseCheckpoint(String checkpoint) {
    if (checkpoint == null || checkpoint.isBlank()) {
      return new String[] {"", "4"};
    }
    int idx = checkpoint.lastIndexOf(':');
    if (idx < 0) {
      return new String[] {checkpoint, "4"};
    }
    return new String[] {checkpoint.substring(0, idx), checkpoint.substring(idx + 1)};
  }

  private Connection openConnection() throws SQLException {
    String jdbc = "jdbc:mysql://" + options.getHost() + ':' + options.getPort() + '/' + options.getDatabase();
    return DriverManager.getConnection(jdbc, options.getUser(), resolvePassword());
  }

  private String readVariable(Connection conn, String name) throws SQLException {
    try (PreparedStatement statement = conn.prepareStatement("SHOW VARIABLES LIKE ?")) {
      statement.setString(1, name);
      try (ResultSet rs = statement.executeQuery()) {
        if (rs.next()) {
          return rs.getString(2);
        }
        return "";
      }
    }
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
}
