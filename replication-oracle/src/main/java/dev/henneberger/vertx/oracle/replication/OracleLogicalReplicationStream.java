package dev.henneberger.vertx.oracle.replication;

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
import io.vertx.core.json.JsonObject;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleLogicalReplicationStream implements ReplicationStream<OracleChangeEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(OracleLogicalReplicationStream.class);

  private final Vertx vertx;
  private final OracleReplicationOptions options;
  private final List<ListenerRegistration> listeners = new CopyOnWriteArrayList<>();
  private final List<Handler<ReplicationStateChange>> stateHandlers = new CopyOnWriteArrayList<>();
  private final List<ReplicationMetricsListener<OracleChangeEvent>> metricsListeners = new CopyOnWriteArrayList<>();
  private final AtomicBoolean shouldRun = new AtomicBoolean(false);
  private volatile Thread worker;
  private volatile Promise<Void> startPromise;
  private volatile ReplicationStreamState state = ReplicationStreamState.CREATED;

  public OracleLogicalReplicationStream(Vertx vertx, OracleReplicationOptions options) {
    this.vertx = Objects.requireNonNull(vertx, "vertx");
    this.options = new OracleReplicationOptions(Objects.requireNonNull(options, "options"));
    this.options.validate();
  }

  @Override
  public Future<Void> start() {
    Promise<Void> promiseToReturn;
    synchronized (this) {
      if (state == ReplicationStreamState.CLOSED) return Future.failedFuture("stream is closed");
      if (state == ReplicationStreamState.RUNNING) return Future.succeededFuture();
      if ((state == ReplicationStreamState.STARTING || state == ReplicationStreamState.RETRYING) && startPromise != null) return startPromise.future();
      shouldRun.set(true);
      startPromise = Promise.promise();
      promiseToReturn = startPromise;
      transition(ReplicationStreamState.STARTING, null, 0);
    }

    Future<Void> preflightFuture = options.isPreflightEnabled() ? preflight().compose(report -> report.ok()
      ? Future.succeededFuture()
      : Future.failedFuture(new IllegalStateException(PreflightReports.describeFailure(report)))) : Future.succeededFuture();

    preflightFuture.onSuccess(v -> startWorker()).onFailure(err -> {
      transition(ReplicationStreamState.FAILED, err, 0);
      shouldRun.set(false);
      failStart(err);
    });

    return promiseToReturn.future();
  }

  @Override
  public Future<PreflightReport> preflight() { return vertx.executeBlocking(this::runPreflightChecks); }
  @Override
  public ReplicationStreamState state() { return state; }

  @Override
  public ReplicationSubscription onStateChange(Handler<ReplicationStateChange> handler) {
    Handler<ReplicationStateChange> resolved = Objects.requireNonNull(handler, "handler");
    stateHandlers.add(resolved);
    return () -> stateHandlers.remove(resolved);
  }

  @Override
  public ReplicationSubscription addMetricsListener(ReplicationMetricsListener<OracleChangeEvent> listener) {
    ReplicationMetricsListener<OracleChangeEvent> resolved = Objects.requireNonNull(listener, "listener");
    metricsListeners.add(resolved);
    return () -> metricsListeners.remove(resolved);
  }

  @Override
  public ReplicationSubscription subscribe(ChangeFilter<OracleChangeEvent> filter,
                                           ChangeConsumer<OracleChangeEvent> eventConsumer,
                                           Handler<Throwable> errorHandler) {
    return registerSubscription(filter, eventConsumer, errorHandler, true);
  }

  public OracleChangeSubscription subscribe(OracleChangeFilter filter,
                                           OracleChangeConsumer eventConsumer,
                                           Handler<Throwable> errorHandler) {
    return registerSubscription(filter, eventConsumer, errorHandler, true);
  }

  @Override
  public SubscriptionRegistration startAndSubscribe(ChangeFilter<OracleChangeEvent> filter,
                                                    ChangeConsumer<OracleChangeEvent> eventConsumer,
                                                    Handler<Throwable> errorHandler) {
    ReplicationSubscription subscription = registerSubscription(filter, eventConsumer, errorHandler, false);
    Future<Void> started = start().onFailure(err -> {
      subscription.cancel();
      if (errorHandler != null) errorHandler.handle(err);
    });
    return new SubscriptionRegistration(subscription, started);
  }

  @Override
  public synchronized void close() {
    shouldRun.set(false);
    transition(ReplicationStreamState.CLOSED, null, 0);
    Thread thread = worker;
    worker = null;
    if (thread != null) thread.interrupt();
    Promise<Void> current = startPromise;
    startPromise = null;
    if (current != null && !current.future().isComplete()) current.fail("stream closed before reaching RUNNING");
  }

  private OracleChangeSubscription registerSubscription(ChangeFilter<OracleChangeEvent> filter,
                                                       ChangeConsumer<OracleChangeEvent> eventConsumer,
                                                       Handler<Throwable> errorHandler,
                                                       boolean withAutoStart) {
    Objects.requireNonNull(filter, "filter");
    Objects.requireNonNull(eventConsumer, "eventConsumer");
    ListenerRegistration registration = new ListenerRegistration(filter, eventConsumer, errorHandler);
    listeners.add(registration);
    if (withAutoStart && options.isAutoStart()) {
      start().onFailure(err -> { if (errorHandler != null) errorHandler.handle(err); });
    }
    return () -> listeners.remove(registration);
  }

  private synchronized void startWorker() {
    if (!shouldRun.get()) return;
    if (worker != null && worker.isAlive()) return;
    worker = new Thread(this::runLoop, "oracle-cdc-" + options.getSourceTable());
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
        if (!shouldRun.get()) return;
        notifyError(e);
        LOG.error("Oracle CDC stream failed for {}", options.getSourceTable(), e);
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
    try (Connection conn = openConnection()) {
      transition(ReplicationStreamState.RUNNING, null, attempt);
      completeStart();
      long lastPosition = parsePosition(options.getLsnStore().load(checkpointKey()).orElse("0"));

      String sql = "SELECT * FROM " + options.getSourceTable() + " WHERE " + options.getPositionColumn() + " > ?"
        + " ORDER BY " + options.getPositionColumn() + " ASC FETCH FIRST ? ROWS ONLY";

      while (shouldRun.get()) {
        List<OracleChangeEvent> events = new ArrayList<>();
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
          statement.setLong(1, lastPosition);
          statement.setInt(2, options.getBatchSize());
          try (ResultSet rs = statement.executeQuery()) {
            while (rs.next()) events.add(mapRow(rs));
          }
        }

        if (events.isEmpty()) {
          sleepInterruptibly(options.getPollIntervalMs());
          continue;
        }

        for (OracleChangeEvent event : events) {
          dispatchAndAwait(event);
          emitEventMetric(event);
          long current = parsePosition(event.getPosition());
          if (current > lastPosition) {
            lastPosition = current;
            String token = Long.toString(lastPosition);
            options.getLsnStore().save(checkpointKey(), token);
            emitLsnCommitted(token);
          }
        }
      }
    }
  }

  private OracleChangeEvent mapRow(ResultSet rs) throws Exception {
    Map<String, Object> row = extractRow(rs);
    String operationRaw = asString(row.get(options.getOperationColumn()));
    OracleChangeEvent.Operation operation = mapOperation(operationRaw);
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
    metadata.put("adapter", "oracle");
    metadata.put("rawOperation", operationRaw);
    return new OracleChangeEvent(options.getSourceTable(), operation, before, after, position, commitTs, metadata);
  }

  private Map<String, Object> extractRow(ResultSet rs) throws Exception {
    ResultSetMetaData md = rs.getMetaData();
    Map<String, Object> out = new LinkedHashMap<>();
    for (int i = 1; i <= md.getColumnCount(); i++) out.put(md.getColumnLabel(i), rs.getObject(i));
    return out;
  }

  private Connection openConnection() throws Exception {
    String url = "jdbc:oracle:thin:@//" + options.getHost() + ":" + options.getPort() + "/" + options.getDatabase() + "";
    return DriverManager.getConnection(url, options.getUser(), resolvePassword());
  }

  private String resolvePassword() {
    String password = options.getPassword();
    if (password == null || password.isBlank()) {
      String env = options.getPasswordEnv();
      if (env != null && !env.isBlank()) password = System.getenv(env);
    }
    return password == null ? "" : password;
  }

  private String checkpointKey() { return "oracle:" + options.getDatabase() + ":" + options.getSourceTable(); }

  private PreflightReport runPreflightChecks() {
    List<PreflightIssue> issues = new ArrayList<>();
    try (Connection conn = openConnection(); PreparedStatement stmt = conn.prepareStatement("SELECT * FROM " + options.getSourceTable() + " WHERE 1=0")) {
      stmt.executeQuery();
    } catch (Exception e) {
      issues.add(new PreflightIssue(PreflightIssue.Severity.ERROR, "SOURCE_TABLE_UNAVAILABLE",
        "Could not access source table  + options.getSourceTable() + : " + e.getMessage(),
        "Verify connectivity, permissions, and sourceTable."));
    }
    return new PreflightReport(issues);
  }

  private static OracleChangeEvent.Operation mapOperation(String op) {
    String n = op == null ? "" : op.trim().toUpperCase(Locale.ROOT);
    if (n.startsWith("INS")) return OracleChangeEvent.Operation.INSERT;
    if (n.startsWith("DEL")) return OracleChangeEvent.Operation.DELETE;
    return OracleChangeEvent.Operation.UPDATE;
  }

  private static Map<String, Object> parseMap(Object raw) {
    if (raw == null) return Collections.emptyMap();
    if (raw instanceof Map) {
      @SuppressWarnings("unchecked") Map<String, Object> map = (Map<String, Object>) raw;
      return map;
    }
    if (raw instanceof JsonObject) return ((JsonObject) raw).getMap();
    if (raw instanceof String) {
      try { return new JsonObject((String) raw).getMap(); } catch (Exception ignore) { }
    }
    return Collections.emptyMap();
  }

  private static Instant toInstant(Object raw) {
    if (raw instanceof Instant) return (Instant) raw;
    if (raw instanceof Timestamp) return ((Timestamp) raw).toInstant();
    if (raw instanceof java.util.Date) return ((java.util.Date) raw).toInstant();
    if (raw instanceof String) {
      try { return Instant.parse((String) raw); } catch (Exception ignore) { }
    }
    return null;
  }

  private static String asString(Object value) { return value == null ? "" : String.valueOf(value); }
  private static long parsePosition(String token) { try { return Long.parseLong(token); } catch (Exception ignore) { return 0L; } }

  private void dispatchAndAwait(OracleChangeEvent event) throws Exception {
    List<ListenerRegistration> matching = new ArrayList<>();
    for (ListenerRegistration l : listeners) if (l.filter.test(event)) matching.add(l);
    if (matching.isEmpty()) return;

    int chunkSize = Math.max(1, options.getMaxConcurrentDispatch());
    for (int start = 0; start < matching.size(); start += chunkSize) {
      int end = Math.min(matching.size(), start + chunkSize);
      CountDownLatch latch = new CountDownLatch(end - start);
      AtomicReference<Throwable> failure = new AtomicReference<>();
      for (int i = start; i < end; i++) {
        ListenerRegistration listener = matching.get(i);
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

  private void invokeListener(OracleChangeEvent event,
                              ListenerRegistration listener,
                              CountDownLatch latch,
                              AtomicReference<Throwable> failure) {
    try {
      Future<Void> result = listener.eventConsumer.handle(event);
      if (result == null) result = Future.succeededFuture();
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

  private void notifyError(Throwable error) {
    for (ListenerRegistration listener : listeners) {
      if (listener.errorHandler != null) vertx.runOnContext(v -> listener.errorHandler.handle(error));
    }
  }

  private void transition(ReplicationStreamState nextState, Throwable cause, long attempt) {
    ReplicationStreamState previous = state;
    if (previous == nextState && cause == null) return;
    state = nextState;
    ReplicationStateChange change = new ReplicationStateChange(previous, nextState, cause, attempt);
    for (ReplicationMetricsListener<OracleChangeEvent> listener : metricsListeners) listener.onStateChange(change);
    for (Handler<ReplicationStateChange> handler : stateHandlers) vertx.runOnContext(v -> handler.handle(change));
  }

  private void emitEventMetric(OracleChangeEvent event) { for (ReplicationMetricsListener<OracleChangeEvent> l : metricsListeners) l.onEvent(event); }
  private void emitLsnCommitted(String token) { for (ReplicationMetricsListener<OracleChangeEvent> l : metricsListeners) l.onLsnCommitted(checkpointKey(), token); }
  private void failStart(Throwable err) { Promise<Void> p = startPromise; if (p != null && !p.future().isComplete()) p.fail(err); }
  private void completeStart() { Promise<Void> p = startPromise; if (p != null && !p.future().isComplete()) p.complete(); }
  private static void sleepInterruptibly(long millis) { try { Thread.sleep(millis); } catch (InterruptedException ignore) { Thread.currentThread().interrupt(); } }

  private static final class ListenerRegistration {
    private final ChangeFilter<OracleChangeEvent> filter;
    private final ChangeConsumer<OracleChangeEvent> eventConsumer;
    private final Handler<Throwable> errorHandler;
    private ListenerRegistration(ChangeFilter<OracleChangeEvent> filter,
                                 ChangeConsumer<OracleChangeEvent> eventConsumer,
                                 Handler<Throwable> errorHandler) {
      this.filter = filter;
      this.eventConsumer = eventConsumer;
      this.errorHandler = errorHandler;
    }
  }
}
