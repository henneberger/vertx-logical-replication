package dev.henneberger.vertx.sqlserver.replication;

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
import dev.henneberger.vertx.replication.core.ValueNormalizationMode;
import dev.henneberger.vertx.replication.core.ValueNormalizer;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.sql.Time;
import java.sql.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlServerLogicalReplicationStream implements ReplicationStream<SqlServerChangeEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(SqlServerLogicalReplicationStream.class);

  private final Vertx vertx;
  private final SqlServerReplicationOptions options;
  private final List<ListenerRegistration> listeners = new CopyOnWriteArrayList<>();
  private final List<Handler<ReplicationStateChange>> stateHandlers = new CopyOnWriteArrayList<>();
  private final List<ReplicationMetricsListener<SqlServerChangeEvent>> metricsListeners = new CopyOnWriteArrayList<>();
  private final AtomicBoolean shouldRun = new AtomicBoolean(false);

  private volatile Thread worker;
  private volatile Promise<Void> startPromise;
  private volatile ReplicationStreamState state = ReplicationStreamState.CREATED;
  private final ValueNormalizer defaultJsonSafeNormalizer = this::normalizeJsonSafe;

  public SqlServerLogicalReplicationStream(Vertx vertx, SqlServerReplicationOptions options) {
    this.vertx = Objects.requireNonNull(vertx, "vertx");
    this.options = new SqlServerReplicationOptions(Objects.requireNonNull(options, "options"));
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

    preflightFuture.onSuccess(v -> startWorker())
      .onFailure(err -> {
        transition(ReplicationStreamState.FAILED, err, 0);
        shouldRun.set(false);
        failStart(err);
      });

    return promiseToReturn.future();
  }

  @Override
  public Future<PreflightReport> preflight() {
    return vertx.executeBlocking(this::runPreflightChecksWithMode);
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
  public ReplicationSubscription addMetricsListener(ReplicationMetricsListener<SqlServerChangeEvent> listener) {
    ReplicationMetricsListener<SqlServerChangeEvent> resolved = Objects.requireNonNull(listener, "listener");
    metricsListeners.add(resolved);
    return () -> metricsListeners.remove(resolved);
  }

  @Override
  public ReplicationSubscription subscribe(ChangeFilter<SqlServerChangeEvent> filter,
                                           ChangeConsumer<SqlServerChangeEvent> eventConsumer,
                                           Handler<Throwable> errorHandler) {
    return registerSubscription(filter, eventConsumer, errorHandler, true);
  }

  public SqlServerChangeSubscription subscribe(SqlServerChangeFilter filter,
                                               SqlServerChangeConsumer eventConsumer,
                                               Handler<Throwable> errorHandler) {
    return registerSubscription(filter, eventConsumer, errorHandler, true);
  }

  @Override
  public SubscriptionRegistration startAndSubscribe(ChangeFilter<SqlServerChangeEvent> filter,
                                                    ChangeConsumer<SqlServerChangeEvent> eventConsumer,
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

  public SubscriptionRegistration startAndSubscribe(SqlServerChangeFilter filter,
                                                    Handler<SqlServerChangeEvent> eventHandler,
                                                    Handler<Throwable> errorHandler) {
    Objects.requireNonNull(eventHandler, "eventHandler");
    return startAndSubscribe(filter, event -> {
      eventHandler.handle(event);
      return Future.succeededFuture();
    }, errorHandler);
  }

  @Override
  public dev.henneberger.vertx.replication.core.AdapterMode adapterMode() {
    return dev.henneberger.vertx.replication.core.AdapterMode.POLLING;
  }

  @Override
  public synchronized void close() {
    shouldRun.set(false);
    transition(ReplicationStreamState.CLOSED, null, 0);

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

  private SqlServerChangeSubscription registerSubscription(ChangeFilter<SqlServerChangeEvent> filter,
                                                           ChangeConsumer<SqlServerChangeEvent> eventConsumer,
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

    worker = new Thread(this::runLoop, "sqlserver-cdc-" + options.getCaptureInstance());
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
      } catch (Exception e) {
        if (!shouldRun.get()) {
          break;
        }

        notifyError(e);
        LOG.error("SQL Server CDC stream failed for captureInstance {}", options.getCaptureInstance(), e);

        RetryPolicy retryPolicy = options.getRetryPolicy();
        if (!retryPolicy.shouldRetry(e, attempt)) {
          transition(ReplicationStreamState.FAILED, e, attempt);
          failStart(e);
          shouldRun.set(false);
          break;
        }

        transition(ReplicationStreamState.RETRYING, e, attempt);
        sleepInterruptibly(retryPolicy.computeDelayMillis(attempt));
      }
    }
    worker = null;
  }

  private void runSession(long attempt) throws Exception {
    try (Connection conn = openConnection()) {
      transition(ReplicationStreamState.RUNNING, null, attempt);
      completeStart();

      String checkpointKey = checkpointKey();
      SqlServerCdcPosition checkpoint = options.getLsnStore()
        .load(checkpointKey)
        .flatMap(SqlServerCdcPosition::parse)
        .orElseGet(() -> SqlServerCdcPosition.initial(fetchMinLsn(conn)));

      while (shouldRun.get()) {
        String toLsn = fetchMaxLsn(conn);
        boolean drained = false;
        while (shouldRun.get() && !drained) {
          PageResult page = fetchChanges(conn, checkpoint, toLsn);
          List<SqlServerChangeEvent> events = page.events;
          if (events.isEmpty()) {
            drained = true;
            continue;
          }
          for (SqlServerChangeEvent event : events) {
            dispatchAndAwait(event);
            emitEventMetric(event);
          }
          checkpoint = page.lastPosition;
          options.getLsnStore().save(checkpointKey, checkpoint.serialize());
          emitLsnCommitted(checkpoint.startLsn());
        }
        if (drained) {
          sleepInterruptibly(options.getPollIntervalMs());
        }
      }
    }
  }

  private Connection openConnection() throws SQLException {
    StringBuilder url = new StringBuilder();
    url.append("jdbc:sqlserver://")
      .append(options.getHost())
      .append(":")
      .append(options.getPort())
      .append(";databaseName=")
      .append(options.getDatabase())
      .append(";encrypt=")
      .append(options.getSsl() ? "true" : "false")
      .append(";trustServerCertificate=true");

    String password = resolvePassword();
    if (password == null) {
      password = "";
    }
    return DriverManager.getConnection(url.toString(), options.getUser(), password);
  }

  private String resolvePassword() {
    String password = options.getPassword();
    if (password == null || password.isBlank()) {
      String envName = options.getPasswordEnv();
      if (envName != null && !envName.isBlank()) {
        password = System.getenv(envName);
      }
    }
    return password;
  }

  private String fetchMinLsn(Connection conn) {
    String sql = "SELECT sys.fn_cdc_get_min_lsn(?)";
    try (PreparedStatement statement = conn.prepareStatement(sql)) {
      statement.setString(1, options.getCaptureInstance());
      try (ResultSet rs = statement.executeQuery()) {
        if (rs.next()) {
          return lsnToHex(rs.getBytes(1));
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Could not fetch min LSN", e);
    }
    return "0x";
  }

  private String fetchMaxLsn(Connection conn) {
    String sql = "SELECT sys.fn_cdc_get_max_lsn()";
    try (PreparedStatement statement = conn.prepareStatement(sql);
         ResultSet rs = statement.executeQuery()) {
      if (rs.next()) {
        return lsnToHex(rs.getBytes(1));
      }
      return "0x";
    } catch (SQLException e) {
      throw new RuntimeException("Could not fetch max LSN", e);
    }
  }

  private PageResult fetchChanges(Connection conn,
                                  SqlServerCdcPosition fromExclusive,
                                  String toLsnInclusive) throws SQLException {
    String fn = "cdc.fn_cdc_get_all_changes_" + options.getCaptureInstance();
    String sql = "SELECT TOP " + options.getMaxBatchSize() + " * FROM " + fn + "(?, ?, 'all update old')"
      + " WHERE (__$start_lsn > ?)"
      + " OR (__$start_lsn = ? AND __$seqval > ?)"
      + " OR (__$start_lsn = ? AND __$seqval = ? AND __$operation > ?)"
      + " ORDER BY __$start_lsn, __$seqval, __$operation";

    List<SqlServerChangeEvent> events = new ArrayList<>();
    SqlServerCdcPosition lastPosition = fromExclusive;
    try (PreparedStatement statement = conn.prepareStatement(sql)) {
      byte[] fromLsn = hexToLsn(fromExclusive.startLsn());
      byte[] fromSeqVal = hexToLsn(fromExclusive.seqVal());
      statement.setBytes(1, fromLsn);
      statement.setBytes(2, hexToLsn(toLsnInclusive));
      statement.setBytes(3, fromLsn);
      statement.setBytes(4, fromLsn);
      statement.setBytes(5, fromSeqVal);
      statement.setBytes(6, fromLsn);
      statement.setBytes(7, fromSeqVal);
      statement.setInt(8, fromExclusive.operation());
      try (ResultSet rs = statement.executeQuery()) {
        while (rs.next()) {
          events.add(mapEvent(rs));
          lastPosition = positionFromRow(rs);
        }
      }
    }
    return new PageResult(events, lastPosition);
  }

  SqlServerChangeEvent mapEvent(ResultSet rs) throws SQLException {
    int opCode = rs.getInt("__$operation");
    SqlServerChangeEvent.Operation op = mapOperation(opCode);

    Map<String, Object> row = extractRowData(rs);
    Map<String, Object> before = new LinkedHashMap<>();
    Map<String, Object> after = new LinkedHashMap<>();
    if (opCode == 1 || opCode == 3) {
      before.putAll(row);
    } else {
      after.putAll(row);
    }

    Map<String, Object> metadata = new LinkedHashMap<>();
    metadata.put("operationCode", opCode);
    metadata.put("seqVal", lsnToHex(rs.getBytes("__$seqval")));

    Instant commitTs = null;
    String lsn = lsnToHex(rs.getBytes("__$start_lsn"));

    return new SqlServerChangeEvent(
      options.getCaptureInstance(),
      op,
      before,
      after,
      lsn,
      commitTs,
      metadata
    );
  }

  private SqlServerCdcPosition positionFromRow(ResultSet rs) throws SQLException {
    return new SqlServerCdcPosition(
      lsnToHex(rs.getBytes("__$start_lsn")),
      lsnToHex(rs.getBytes("__$seqval")),
      rs.getInt("__$operation")
    );
  }

  static SqlServerChangeEvent.Operation mapOperation(int opCode) {
    switch (opCode) {
      case 1:
        return SqlServerChangeEvent.Operation.DELETE;
      case 2:
        return SqlServerChangeEvent.Operation.INSERT;
      case 3:
      case 4:
        return SqlServerChangeEvent.Operation.UPDATE;
      default:
        throw new IllegalArgumentException("Unsupported CDC operation code: " + opCode);
    }
  }

  private Map<String, Object> extractRowData(ResultSet rs) throws SQLException {
    ResultSetMetaData metaData = rs.getMetaData();
    Map<String, Object> data = new LinkedHashMap<>();
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      String name = metaData.getColumnName(i);
      if (name.startsWith("__$")) {
        continue;
      }
      data.put(name, normalizeValue(name, rs.getObject(i)));
    }
    return data;
  }

  Object normalizeValue(String fieldName, Object value) {
    ValueNormalizationMode mode = options.valueNormalizationMode();
    if (mode == ValueNormalizationMode.RAW) {
      return value;
    }
    if (mode == ValueNormalizationMode.CUSTOM) {
      ValueNormalizer normalizer = options.getValueNormalizer();
      return normalizer == null ? value : normalizer.normalize(fieldName, value);
    }
    return defaultJsonSafeNormalizer.normalize(fieldName, value);
  }

  private Object normalizeJsonSafe(String fieldName, Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Timestamp) {
      return ((Timestamp) value).toInstant().toString();
    }
    if (value instanceof Date) {
      return ((Date) value).toLocalDate().toString();
    }
    if (value instanceof Time) {
      return ((Time) value).toLocalTime().format(DateTimeFormatter.ISO_LOCAL_TIME);
    }
    if (value instanceof OffsetDateTime) {
      return ((OffsetDateTime) value).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }
    if (value instanceof LocalDate) {
      return value.toString();
    }
    if (value instanceof LocalTime) {
      return ((LocalTime) value).format(DateTimeFormatter.ISO_LOCAL_TIME);
    }
    return value;
  }

  private void dispatchAndAwait(SqlServerChangeEvent event) throws Exception {
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
        if (err instanceof Exception) {
          throw (Exception) err;
        }
        throw new RuntimeException(err);
      }
    }
  }

  private void invokeListener(SqlServerChangeEvent event,
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
          if (listener.errorHandler != null) {
            listener.errorHandler.handle(err);
          }
          failure.compareAndSet(null, err);
        }
        latch.countDown();
      });
    } catch (Throwable err) {
      if (listener.errorHandler != null) {
        listener.errorHandler.handle(err);
      }
      failure.compareAndSet(null, err);
      latch.countDown();
    }
  }

  private void notifyError(Throwable error) {
    for (ListenerRegistration listener : listeners) {
      if (listener.errorHandler != null) {
        vertx.runOnContext(v -> listener.errorHandler.handle(error));
      }
    }
  }

  private void transition(ReplicationStreamState nextState, Throwable cause, long attempt) {
    ReplicationStreamState previous = state;
    if (previous == nextState && cause == null) {
      return;
    }
    state = nextState;

    ReplicationStateChange change = new ReplicationStateChange(previous, nextState, cause, attempt);
    for (ReplicationMetricsListener<SqlServerChangeEvent> listener : metricsListeners) {
      listener.onStateChange(change);
    }
    for (Handler<ReplicationStateChange> handler : stateHandlers) {
      vertx.runOnContext(v -> handler.handle(change));
    }
  }

  private void emitEventMetric(SqlServerChangeEvent event) {
    for (ReplicationMetricsListener<SqlServerChangeEvent> listener : metricsListeners) {
      listener.onEvent(event);
    }
  }

  private void emitLsnCommitted(String lsn) {
    for (ReplicationMetricsListener<SqlServerChangeEvent> listener : metricsListeners) {
      listener.onLsnCommitted(checkpointKey(), lsn);
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

  private String checkpointKey() {
    return "sqlserver:" + options.getDatabase() + ':' + options.getCaptureInstance();
  }

  private PreflightReport runPreflightChecksWithMode() {
    if (!"wait-until-ready".equalsIgnoreCase(options.getPreflightMode())) {
      return runPreflightChecks();
    }

    long deadline = System.currentTimeMillis() + options.getPreflightMaxWaitMs();
    PreflightReport latest = runPreflightChecks();
    while (!latest.ok() && System.currentTimeMillis() < deadline) {
      sleepInterruptibly(options.getPreflightRetryIntervalMs());
      latest = runPreflightChecks();
    }
    return latest;
  }

  private PreflightReport runPreflightChecks() {
    List<PreflightIssue> issues = new ArrayList<>();

    try (Connection conn = openConnection()) {
      if (!isCdcEnabled(conn)) {
        issues.add(new PreflightIssue(
          PreflightIssue.Severity.ERROR,
          "CDC_NOT_ENABLED",
          "CDC is not enabled for database '" + options.getDatabase() + "'",
          "Enable CDC using sys.sp_cdc_enable_db before starting replication."
        ));
      }
      if (!captureInstanceExists(conn)) {
        issues.add(new PreflightIssue(
          PreflightIssue.Severity.ERROR,
          "CAPTURE_INSTANCE_MISSING",
          "Capture instance '" + options.getCaptureInstance() + "' was not found",
          "Enable CDC on the target table and use the configured capture instance name."
        ));
      }
    } catch (Exception e) {
      issues.add(new PreflightIssue(
        PreflightIssue.Severity.ERROR,
        "CONNECTION_FAILED",
        "Could not connect to SQL Server: " + e.getMessage(),
        "Verify host, port, database, user, password, and SSL settings."
      ));
    }

    return new PreflightReport(issues);
  }

  private boolean isCdcEnabled(Connection conn) throws SQLException {
    try (PreparedStatement statement = conn.prepareStatement("SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()");
         ResultSet rs = statement.executeQuery()) {
      return rs.next() && rs.getBoolean(1);
    }
  }

  private boolean captureInstanceExists(Connection conn) throws SQLException {
    try (PreparedStatement statement = conn.prepareStatement("SELECT 1 FROM cdc.change_tables WHERE capture_instance = ?")) {
      statement.setString(1, options.getCaptureInstance().toLowerCase(Locale.ROOT));
      try (ResultSet rs = statement.executeQuery()) {
        return rs.next();
      }
    }
  }

  private static void sleepInterruptibly(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ignore) {
      Thread.currentThread().interrupt();
    }
  }

  static String lsnToHex(byte[] lsn) {
    if (lsn == null || lsn.length == 0) {
      return "0x";
    }
    StringBuilder sb = new StringBuilder("0x");
    for (byte b : lsn) {
      sb.append(String.format("%02X", b));
    }
    return sb.toString();
  }

  static byte[] hexToLsn(String hex) {
    if (hex == null || hex.isBlank() || "0x".equalsIgnoreCase(hex)) {
      return new byte[0];
    }
    String clean = hex.startsWith("0x") || hex.startsWith("0X") ? hex.substring(2) : hex;
    if ((clean.length() % 2) != 0) {
      clean = "0" + clean;
    }
    byte[] bytes = new byte[clean.length() / 2];
    for (int i = 0; i < clean.length(); i += 2) {
      bytes[i / 2] = (byte) Integer.parseInt(clean.substring(i, i + 2), 16);
    }
    return bytes;
  }

  static int compareLsn(String left, String right) {
    byte[] a = hexToLsn(left);
    byte[] b = hexToLsn(right);

    int max = Math.max(a.length, b.length);
    for (int i = 0; i < max; i++) {
      int ai = i < (max - a.length) ? 0 : a[i - (max - a.length)] & 0xFF;
      int bi = i < (max - b.length) ? 0 : b[i - (max - b.length)] & 0xFF;
      if (ai != bi) {
        return Integer.compare(ai, bi);
      }
    }
    return 0;
  }

  private static final class ListenerRegistration {
    private final ChangeFilter<SqlServerChangeEvent> filter;
    private final ChangeConsumer<SqlServerChangeEvent> eventConsumer;
    private final Handler<Throwable> errorHandler;

    private ListenerRegistration(ChangeFilter<SqlServerChangeEvent> filter,
                                 ChangeConsumer<SqlServerChangeEvent> eventConsumer,
                                 Handler<Throwable> errorHandler) {
      this.filter = filter;
      this.eventConsumer = eventConsumer;
      this.errorHandler = errorHandler;
    }
  }

  private static final class PageResult {
    private final List<SqlServerChangeEvent> events;
    private final SqlServerCdcPosition lastPosition;

    private PageResult(List<SqlServerChangeEvent> events, SqlServerCdcPosition lastPosition) {
      this.events = events;
      this.lastPosition = lastPosition;
    }
  }
}
