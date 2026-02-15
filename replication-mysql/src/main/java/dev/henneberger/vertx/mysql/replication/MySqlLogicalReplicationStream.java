package dev.henneberger.vertx.mysql.replication;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import dev.henneberger.vertx.replication.core.ChangeConsumer;
import dev.henneberger.vertx.replication.core.ChangeFilter;
import dev.henneberger.vertx.replication.core.PreflightIssue;
import dev.henneberger.vertx.replication.core.PreflightReport;
import dev.henneberger.vertx.replication.core.PreflightReports;
import dev.henneberger.vertx.replication.core.ReplicationMetricsListener;
import dev.henneberger.vertx.replication.core.ReplicationStateChange;
import dev.henneberger.vertx.replication.core.ReplicationStream;
import dev.henneberger.vertx.replication.core.ReplicationStreamState;
import dev.henneberger.vertx.replication.core.ReplicationSubscription;
import dev.henneberger.vertx.replication.core.RetryPolicy;
import dev.henneberger.vertx.replication.core.SubscriptionRegistration;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import java.io.Serializable;
import java.sql.Connection;
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
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySqlLogicalReplicationStream implements ReplicationStream<MySqlChangeEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(MySqlLogicalReplicationStream.class);

  private final Vertx vertx;
  private final MySqlReplicationOptions options;
  private final List<ListenerRegistration> listeners = new CopyOnWriteArrayList<>();
  private final List<Handler<ReplicationStateChange>> stateHandlers = new CopyOnWriteArrayList<>();
  private final List<ReplicationMetricsListener<MySqlChangeEvent>> metricsListeners = new CopyOnWriteArrayList<>();
  private final AtomicBoolean shouldRun = new AtomicBoolean(false);
  private final Map<Long, String> tableNames = new HashMap<>();

  private volatile Thread worker;
  private volatile Promise<Void> startPromise;
  private volatile ReplicationStreamState state = ReplicationStreamState.CREATED;
  private volatile BinaryLogClient client;

  public MySqlLogicalReplicationStream(Vertx vertx, MySqlReplicationOptions options) {
    this.vertx = Objects.requireNonNull(vertx, "vertx");
    this.options = new MySqlReplicationOptions(Objects.requireNonNull(options, "options"));
    this.options.validate();
  }

  @Override
  public Future<Void> start() {
    Promise<Void> promiseToReturn;
    synchronized (this) {
      if (state == ReplicationStreamState.CLOSED) {
        return Future.failedFuture("stream is closed");
      }
      if (state == ReplicationStreamState.RUNNING) {
        return Future.succeededFuture();
      }
      if ((state == ReplicationStreamState.STARTING || state == ReplicationStreamState.RETRYING)
        && startPromise != null) {
        return startPromise.future();
      }

      shouldRun.set(true);
      startPromise = Promise.promise();
      promiseToReturn = startPromise;
      transition(ReplicationStreamState.STARTING, null, 0);
    }

    Future<Void> preflightFuture;
    if (options.isPreflightEnabled()) {
      preflightFuture = preflight().compose(report -> report.ok()
        ? Future.succeededFuture()
        : Future.failedFuture(new dev.henneberger.vertx.replication.core.PreflightFailedException(report, ReplicationStreamState.STARTING)));
    } else {
      preflightFuture = Future.succeededFuture();
    }

    preflightFuture.onSuccess(v -> startWorker()).onFailure(err -> {
      transition(ReplicationStreamState.FAILED, err, 0);
      shouldRun.set(false);
      failStart(err);
    });

    return promiseToReturn.future();
  }

  @Override
  public Future<PreflightReport> preflight() {
    return vertx.executeBlocking(this::runPreflightChecks);
  }

  @Override
  public ReplicationStreamState state() {
    return state;
  }

  @Override
  public ReplicationSubscription onStateChange(Handler<ReplicationStateChange> handler) {
    Handler<ReplicationStateChange> resolved = Objects.requireNonNull(handler, "handler");
    stateHandlers.add(resolved);
    return () -> stateHandlers.remove(resolved);
  }

  @Override
  public ReplicationSubscription addMetricsListener(ReplicationMetricsListener<MySqlChangeEvent> listener) {
    ReplicationMetricsListener<MySqlChangeEvent> resolved = Objects.requireNonNull(listener, "listener");
    metricsListeners.add(resolved);
    return () -> metricsListeners.remove(resolved);
  }

  @Override
  public ReplicationSubscription subscribe(ChangeFilter<MySqlChangeEvent> filter,
                                           ChangeConsumer<MySqlChangeEvent> eventConsumer,
                                           Handler<Throwable> errorHandler) {
    return registerSubscription(filter, eventConsumer, errorHandler, true);
  }

  @Override
  public SubscriptionRegistration startAndSubscribe(ChangeFilter<MySqlChangeEvent> filter,
                                                    ChangeConsumer<MySqlChangeEvent> eventConsumer,
                                                    Handler<Throwable> errorHandler) {
    ReplicationSubscription subscription = registerSubscription(filter, eventConsumer, errorHandler, false);
    Future<Void> started = start().onFailure(err -> {
      subscription.cancel();
      if (errorHandler != null) {
        errorHandler.handle(err);
      }
    });
    return new SubscriptionRegistration(subscription, started);
  }

  @Override
  public dev.henneberger.vertx.replication.core.AdapterMode adapterMode() {
    return dev.henneberger.vertx.replication.core.AdapterMode.LOG_STREAM;
  }

  @Override
  public synchronized void close() {
    shouldRun.set(false);
    transition(ReplicationStreamState.CLOSED, null, 0);

    BinaryLogClient currentClient = client;
    client = null;
    if (currentClient != null) {
      try {
        currentClient.disconnect();
      } catch (Exception ignore) {
      }
    }

    Thread thread = worker;
    worker = null;
    if (thread != null) {
      thread.interrupt();
    }

    Promise<Void> currentStartPromise = startPromise;
    startPromise = null;
    if (currentStartPromise != null && !currentStartPromise.future().isComplete()) {
      currentStartPromise.fail("stream closed before reaching RUNNING");
    }
  }

  private MySqlChangeSubscription registerSubscription(ChangeFilter<MySqlChangeEvent> filter,
                                                       ChangeConsumer<MySqlChangeEvent> eventConsumer,
                                                       Handler<Throwable> errorHandler,
                                                       boolean withAutoStart) {
    Objects.requireNonNull(filter, "filter");
    Objects.requireNonNull(eventConsumer, "eventConsumer");

    ListenerRegistration registration = new ListenerRegistration(filter, eventConsumer, errorHandler);
    listeners.add(registration);

    if (withAutoStart && options.isAutoStart()) {
      start().onFailure(err -> {
        if (errorHandler != null) {
          errorHandler.handle(err);
        }
      });
    }

    return () -> listeners.remove(registration);
  }

  private synchronized void startWorker() {
    if (!shouldRun.get()) {
      return;
    }
    if (worker != null && worker.isAlive()) {
      return;
    }

    worker = new Thread(this::runLoop, "mysql-cdc-" + options.getDatabase());
    worker.setDaemon(true);
    worker.start();
  }

  private void runLoop() {
    long attempt = 0;
    while (shouldRun.get()) {
      attempt++;
      transition(ReplicationStreamState.STARTING, null, attempt);
      try {
        runSession(attempt);
        if (!shouldRun.get()) {
          return;
        }
      } catch (Exception e) {
        if (!shouldRun.get()) {
          return;
        }

        notifyError(e);
        LOG.error("MySQL CDC stream failed for database {}", options.getDatabase(), e);

        RetryPolicy retryPolicy = options.getRetryPolicy();
        if (!retryPolicy.shouldRetry(e, attempt)) {
          transition(ReplicationStreamState.FAILED, e, attempt);
          failStart(e);
          shouldRun.set(false);
          return;
        }

        transition(ReplicationStreamState.RETRYING, e, attempt);
        sleepInterruptibly(retryPolicy.computeDelayMillis(attempt));
      }
    }
  }

  private void runSession(long attempt) throws Exception {
    BinaryLogClient c = new BinaryLogClient(options.getHost(), options.getPort(), options.getUser(), resolvePassword());
    c.setServerId(options.getServerId());
    c.setKeepAlive(true);
    c.setConnectTimeout((int) options.getConnectTimeoutMs());

    Optional<String> checkpoint = options.getLsnStore().load(checkpointKey());
    checkpoint.ifPresent(token -> {
      String[] parsed = parseCheckpoint(token);
      c.setBinlogFilename(parsed[0]);
      c.setBinlogPosition(Long.parseLong(parsed[1]));
    });

    c.registerEventListener(event -> handleEvent(c, event));

    client = c;
    transition(ReplicationStreamState.RUNNING, null, attempt);
    completeStart();

    c.connect();
  }

  private void handleEvent(BinaryLogClient c, Event event) {
    if (!shouldRun.get()) {
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
            Collections.emptyMap(), rowMap(row), c.getBinlogFilename(), header.getNextPosition()));
        }
      } else if (data instanceof UpdateRowsEventData) {
        UpdateRowsEventData ur = (UpdateRowsEventData) data;
        String table = tableName(ur.getTableId());
        for (Map.Entry<Serializable[], Serializable[]> entry : ur.getRows()) {
          emitChange(new MySqlChangeEvent(table, MySqlChangeEvent.Operation.UPDATE,
            rowMap(entry.getKey()), rowMap(entry.getValue()), c.getBinlogFilename(), header.getNextPosition()));
        }
      } else if (data instanceof DeleteRowsEventData) {
        DeleteRowsEventData dr = (DeleteRowsEventData) data;
        String table = tableName(dr.getTableId());
        for (Serializable[] row : dr.getRows()) {
          emitChange(new MySqlChangeEvent(table, MySqlChangeEvent.Operation.DELETE,
            rowMap(row), Collections.emptyMap(), c.getBinlogFilename(), header.getNextPosition()));
        }
      }
    } catch (Exception e) {
      emitParseFailure("binlog-event", e);
      notifyError(e);
      try {
        c.disconnect();
      } catch (Exception ignore) {
      }
    }
  }

  private void emitChange(MySqlChangeEvent event) throws Exception {
    dispatchAndAwait(event);
    emitEventMetric(event);

    String token = formatCheckpoint(event.getBinlogFile(), event.getBinlogPosition());
    options.getLsnStore().save(checkpointKey(), token);
    emitLsnCommitted(token);
  }

  private void dispatchAndAwait(MySqlChangeEvent event) throws Exception {
    List<ListenerRegistration> matching = new ArrayList<>();
    for (ListenerRegistration listener : listeners) {
      if (listener.filter.test(event)) {
        matching.add(listener);
      }
    }
    if (matching.isEmpty()) {
      return;
    }

    int chunkSize = Math.max(1, options.getMaxConcurrentDispatch());
    for (int start = 0; start < matching.size(); start += chunkSize) {
      int end = Math.min(matching.size(), start + chunkSize);
      CountDownLatch latch = new CountDownLatch(end - start);
      AtomicReference<Throwable> failure = new AtomicReference<>();

      for (int idx = start; idx < end; idx++) {
        ListenerRegistration listener = matching.get(idx);
        vertx.runOnContext(v -> invokeListener(event, listener, latch, failure));
      }

      latch.await();
      Throwable err = failure.get();
      if (err != null) {
        if (err instanceof Exception) throw (Exception) err;
        throw new RuntimeException(err);
      }
    }
  }

  private void invokeListener(MySqlChangeEvent event,
                              ListenerRegistration listener,
                              CountDownLatch latch,
                              AtomicReference<Throwable> failure) {
    try {
      Future<Void> result = listener.eventConsumer.handle(event);
      if (result == null) {
        result = Future.succeededFuture();
      }
      result.onComplete(ar -> {
        if (ar.failed()) {
          Throwable err = ar.cause();
          if (listener.errorHandler != null) listener.errorHandler.handle(err);
          failure.compareAndSet(null, err);
        }
        latch.countDown();
      });
    } catch (Throwable err) {
      if (listener.errorHandler != null) listener.errorHandler.handle(err);
      failure.compareAndSet(null, err);
      latch.countDown();
    }
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
    if (value == null) return null;
    if (value instanceof Timestamp) return ((Timestamp) value).toInstant().toString();
    if (value instanceof Date) return ((Date) value).toInstant().toString();
    if (value instanceof LocalDateTime) return ((LocalDateTime) value).toString();
    if (value instanceof LocalDate) return ((LocalDate) value).toString();
    if (value instanceof LocalTime) return ((LocalTime) value).toString();
    if (value instanceof byte[]) return new String((byte[]) value);
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

  private PreflightReport runPreflightChecks() {
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

  private void transition(ReplicationStreamState nextState, Throwable cause, long attempt) {
    ReplicationStreamState previous = state;
    if (previous == nextState && cause == null) {
      return;
    }
    state = nextState;

    ReplicationStateChange change = new ReplicationStateChange(previous, nextState, cause, attempt);
    for (ReplicationMetricsListener<MySqlChangeEvent> listener : metricsListeners) {
      listener.onStateChange(change);
    }
    for (Handler<ReplicationStateChange> handler : stateHandlers) {
      vertx.runOnContext(v -> handler.handle(change));
    }
  }

  private void emitEventMetric(MySqlChangeEvent event) {
    for (ReplicationMetricsListener<MySqlChangeEvent> listener : metricsListeners) {
      listener.onEvent(event);
    }
  }

  private void emitParseFailure(String payload, Throwable error) {
    for (ReplicationMetricsListener<MySqlChangeEvent> listener : metricsListeners) {
      listener.onParseFailure(payload, error);
    }
  }

  private void emitLsnCommitted(String token) {
    for (ReplicationMetricsListener<MySqlChangeEvent> listener : metricsListeners) {
      listener.onLsnCommitted(checkpointKey(), token);
    }
  }

  private void notifyError(Throwable error) {
    for (ListenerRegistration listener : listeners) {
      if (listener.errorHandler != null) {
        vertx.runOnContext(v -> listener.errorHandler.handle(error));
      }
    }
  }

  private void failStart(Throwable err) {
    Promise<Void> promise = startPromise;
    if (promise != null && !promise.future().isComplete()) {
      promise.fail(err);
    }
  }

  private void completeStart() {
    Promise<Void> promise = startPromise;
    if (promise != null && !promise.future().isComplete()) {
      promise.complete();
    }
  }

  private static void sleepInterruptibly(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ignored) {
      Thread.currentThread().interrupt();
    }
  }

  private static final class ListenerRegistration {
    private final ChangeFilter<MySqlChangeEvent> filter;
    private final ChangeConsumer<MySqlChangeEvent> eventConsumer;
    private final Handler<Throwable> errorHandler;

    private ListenerRegistration(ChangeFilter<MySqlChangeEvent> filter,
                                 ChangeConsumer<MySqlChangeEvent> eventConsumer,
                                 Handler<Throwable> errorHandler) {
      this.filter = filter;
      this.eventConsumer = eventConsumer;
      this.errorHandler = errorHandler;
    }
  }
}
