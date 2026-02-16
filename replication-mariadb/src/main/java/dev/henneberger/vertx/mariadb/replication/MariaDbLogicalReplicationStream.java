package dev.henneberger.vertx.mariadb.replication;

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
import dev.henneberger.vertx.replication.core.ReplicationStateChange;
import dev.henneberger.vertx.replication.core.RetryPolicy;
import dev.henneberger.vertx.replication.core.SubscriptionRegistration;
import io.vertx.core.Future;
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
import java.time.Instant;
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

public class MariaDbLogicalReplicationStream extends AbstractWorkerReplicationStream<MariaDbChangeEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(MariaDbLogicalReplicationStream.class);

  private final MariaDbReplicationOptions options;
  private final Map<Long, TableRef> tableRefs = new HashMap<>();

  private volatile BinaryLogClient client;

  public MariaDbLogicalReplicationStream(Vertx vertx, MariaDbReplicationOptions options) {
    super(vertx);
    this.options = new MariaDbReplicationOptions(Objects.requireNonNull(options, "options"));
    this.options.validate();
  }

  public MariaDbChangeSubscription subscribe(MariaDbChangeFilter filter,
                                             MariaDbChangeConsumer eventConsumer,
                                             Handler<Throwable> errorHandler) {
    return () -> registerSubscription(filter, eventConsumer, errorHandler, true).cancel();
  }

  public SubscriptionRegistration startAndSubscribe(MariaDbChangeFilter filter,
                                                    Handler<MariaDbChangeEvent> eventHandler,
                                                    Handler<Throwable> errorHandler) {
    Objects.requireNonNull(eventHandler, "eventHandler");
    return startAndSubscribe(filter, event -> {
      eventHandler.handle(event);
      return Future.succeededFuture();
    }, errorHandler);
  }

  public SubscriptionRegistration startAndSubscribe(MariaDbChangeFilter filter,
                                                    MariaDbChangeConsumer eventConsumer,
                                                    Handler<Throwable> errorHandler) {
    return startAndSubscribe((ChangeFilter<MariaDbChangeEvent>) filter, eventConsumer, errorHandler);
  }

  @Override
  public MariaDbChangeSubscription subscribe(ChangeFilter<MariaDbChangeEvent> filter,
                                             ChangeConsumer<MariaDbChangeEvent> eventConsumer,
                                             Handler<Throwable> errorHandler) {
    return () -> registerSubscription(filter, eventConsumer, errorHandler, true).cancel();
  }

  @Override
  public AdapterMode adapterMode() {
    return AdapterMode.LOG_STREAM;
  }

  @Override
  protected String streamName() {
    return "mariadb-cdc-" + options.getDatabase();
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
        "Could not connect to MariaDB: " + e.getMessage(),
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
    transition(dev.henneberger.vertx.replication.core.ReplicationStreamState.RUNNING, null, attempt);
    completeStart();

    try {
      localClient.connect();
    } finally {
      client = null;
    }
  }

  @Override
  protected void logStreamFailure(Throwable error) {
    LOG.error("MariaDB CDC stream failed for database {}", options.getDatabase(), error);
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
        tableRefs.put(t.getTableId(), new TableRef(t.getDatabase(), t.getTable()));
        return;
      }

      EventHeaderV4 header = (EventHeaderV4) event.getHeader();
      if (data instanceof WriteRowsEventData) {
        WriteRowsEventData wr = (WriteRowsEventData) data;
        TableRef tableRef = tableRef(wr.getTableId());
        if (!isTrackedSource(tableRef)) {
          return;
        }
        for (Serializable[] row : wr.getRows()) {
          emitChange(new MariaDbChangeEvent(
            tableRef.table,
            MariaDbChangeEvent.Operation.INSERT,
            Collections.emptyMap(),
            rowMap(row),
            formatCheckpoint(localClient.getBinlogFilename(), header.getNextPosition()),
            toInstant(header.getTimestamp()),
            metadata(tableRef, localClient.getBinlogFilename(), header.getNextPosition(), "INSERT")
          ));
        }
      } else if (data instanceof UpdateRowsEventData) {
        UpdateRowsEventData ur = (UpdateRowsEventData) data;
        TableRef tableRef = tableRef(ur.getTableId());
        if (!isTrackedSource(tableRef)) {
          return;
        }
        for (Map.Entry<Serializable[], Serializable[]> entry : ur.getRows()) {
          emitChange(new MariaDbChangeEvent(
            tableRef.table,
            MariaDbChangeEvent.Operation.UPDATE,
            rowMap(entry.getKey()),
            rowMap(entry.getValue()),
            formatCheckpoint(localClient.getBinlogFilename(), header.getNextPosition()),
            toInstant(header.getTimestamp()),
            metadata(tableRef, localClient.getBinlogFilename(), header.getNextPosition(), "UPDATE")
          ));
        }
      } else if (data instanceof DeleteRowsEventData) {
        DeleteRowsEventData dr = (DeleteRowsEventData) data;
        TableRef tableRef = tableRef(dr.getTableId());
        if (!isTrackedSource(tableRef)) {
          return;
        }
        for (Serializable[] row : dr.getRows()) {
          emitChange(new MariaDbChangeEvent(
            tableRef.table,
            MariaDbChangeEvent.Operation.DELETE,
            rowMap(row),
            Collections.emptyMap(),
            formatCheckpoint(localClient.getBinlogFilename(), header.getNextPosition()),
            toInstant(header.getTimestamp()),
            metadata(tableRef, localClient.getBinlogFilename(), header.getNextPosition(), "DELETE")
          ));
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

  private void emitChange(MariaDbChangeEvent event) throws Exception {
    dispatchAndAwait(event);
    emitEventMetric(event);

    String token = event.getPosition();
    saveCheckpoint(checkpointKey(), token);
    emitLsnCommitted(checkpointKey(), token);
  }

  private TableRef tableRef(long tableId) {
    return tableRefs.getOrDefault(tableId, new TableRef("unknown", "unknown"));
  }

  private boolean isTrackedSource(TableRef tableRef) {
    String configured = options.getSourceTable();
    if (configured == null || configured.isBlank()) {
      return true;
    }
    String full = tableRef.database + "." + tableRef.table;
    return configured.equalsIgnoreCase(tableRef.table) || configured.equalsIgnoreCase(full);
  }

  private static Map<String, Object> metadata(TableRef tableRef, String binlogFile, long binlogPosition, String op) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("adapter", "mariadb");
    out.put("database", tableRef.database);
    out.put("table", tableRef.table);
    out.put("fullTable", tableRef.database + "." + tableRef.table);
    out.put("binlogFile", binlogFile);
    out.put("binlogPosition", binlogPosition);
    out.put("rawOperation", op);
    return out;
  }

  private static Instant toInstant(long headerTimestamp) {
    if (headerTimestamp <= 0) {
      return null;
    }
    return Instant.ofEpochMilli(headerTimestamp);
  }

  private static Map<String, Object> rowMap(Serializable[] row) {
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
    return "mariadb:" + options.getDatabase();
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
    String jdbc = "jdbc:mariadb://" + options.getHost() + ':' + options.getPort() + '/' + options.getDatabase();
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

  private static final class TableRef {
    private final String database;
    private final String table;

    private TableRef(String database, String table) {
      this.database = database;
      this.table = table;
    }
  }
}
