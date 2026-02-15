package dev.henneberger.vertx.scylladb.replication;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.CqlIdentifier;
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
import java.net.InetSocketAddress;
import java.time.Instant;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScyllaDbLogicalReplicationStream implements ReplicationStream<ScyllaDbChangeEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(ScyllaDbLogicalReplicationStream.class);

  private final Vertx vertx;
  private final ScyllaDbReplicationOptions options;
  private final List<ListenerRegistration> listeners = new CopyOnWriteArrayList<>();
  private final List<Handler<ReplicationStateChange>> stateHandlers = new CopyOnWriteArrayList<>();
  private final List<ReplicationMetricsListener<ScyllaDbChangeEvent>> metricsListeners = new CopyOnWriteArrayList<>();
  private final AtomicBoolean shouldRun = new AtomicBoolean(false);

  private volatile Thread worker;
  private volatile Promise<Void> startPromise;
  private volatile ReplicationStreamState state = ReplicationStreamState.CREATED;
  private volatile CqlSession session;

  public ScyllaDbLogicalReplicationStream(Vertx vertx, ScyllaDbReplicationOptions options) {
    this.vertx = Objects.requireNonNull(vertx, "vertx");
    this.options = new ScyllaDbReplicationOptions(Objects.requireNonNull(options, "options"));
    this.options.validate();
  }

  @Override
  public Future<Void> start() {
    Promise<Void> promiseToReturn;
    synchronized (this) {
      if (state == ReplicationStreamState.CLOSED) return Future.failedFuture("stream is closed");
      if (state == ReplicationStreamState.RUNNING) return Future.succeededFuture();
      if ((state == ReplicationStreamState.STARTING || state == ReplicationStreamState.RETRYING)
        && startPromise != null) return startPromise.future();

      shouldRun.set(true);
      startPromise = Promise.promise();
      promiseToReturn = startPromise;
      transition(ReplicationStreamState.STARTING, null, 0);
    }

    Future<Void> preflightFuture = options.isPreflightEnabled()
      ? preflight().compose(report -> report.ok()
      ? Future.succeededFuture()
      : Future.failedFuture(new IllegalStateException(PreflightReports.describeFailure(report))))
      : Future.succeededFuture();

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
  public ReplicationSubscription addMetricsListener(ReplicationMetricsListener<ScyllaDbChangeEvent> listener) {
    ReplicationMetricsListener<ScyllaDbChangeEvent> resolved = Objects.requireNonNull(listener, "listener");
    metricsListeners.add(resolved);
    return () -> metricsListeners.remove(resolved);
  }

  @Override
  public ReplicationSubscription subscribe(ChangeFilter<ScyllaDbChangeEvent> filter,
                                           ChangeConsumer<ScyllaDbChangeEvent> eventConsumer,
                                           Handler<Throwable> errorHandler) {
    return registerSubscription(filter, eventConsumer, errorHandler, true);
  }

  public ScyllaDbChangeSubscription subscribe(ScyllaDbChangeFilter filter,
                                               ScyllaDbChangeConsumer eventConsumer,
                                               Handler<Throwable> errorHandler) {
    return registerSubscription(filter, eventConsumer, errorHandler, true);
  }

  @Override
  public SubscriptionRegistration startAndSubscribe(ChangeFilter<ScyllaDbChangeEvent> filter,
                                                    ChangeConsumer<ScyllaDbChangeEvent> eventConsumer,
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
    Thread t = worker;
    worker = null;
    if (t != null) t.interrupt();

    CqlSession current = session;
    session = null;
    if (current != null) current.close();

    Promise<Void> currentStartPromise = startPromise;
    startPromise = null;
    if (currentStartPromise != null && !currentStartPromise.future().isComplete()) {
      currentStartPromise.fail("stream closed before reaching RUNNING");
    }
  }

  private ScyllaDbChangeSubscription registerSubscription(ChangeFilter<ScyllaDbChangeEvent> filter,
                                                           ChangeConsumer<ScyllaDbChangeEvent> eventConsumer,
                                                           Handler<Throwable> errorHandler,
                                                           boolean withAutoStart) {
    Objects.requireNonNull(filter, "filter");
    Objects.requireNonNull(eventConsumer, "eventConsumer");
    ListenerRegistration registration = new ListenerRegistration(filter, eventConsumer, errorHandler);
    listeners.add(registration);
    if (withAutoStart && options.isAutoStart()) {
      start().onFailure(err -> {
        if (errorHandler != null) errorHandler.handle(err);
      });
    }
    return () -> listeners.remove(registration);
  }

  private synchronized void startWorker() {
    if (!shouldRun.get()) return;
    if (worker != null && worker.isAlive()) return;
    worker = new Thread(this::runLoop, "scylladb-cdc-" + options.getSourceTable());
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
        LOG.error("ScyllaDb CDC stream failed for {}.{}", options.getKeyspace(), options.getSourceTable(), e);
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
    try (CqlSession localSession = openSession()) {
      session = localSession;
      String cql = "SELECT * FROM " + options.getKeyspace() + "." + options.getSourceTable()
        + " WHERE " + options.getPositionColumn() + " > ? LIMIT ? ALLOW FILTERING";
      PreparedStatement statement = localSession.prepare(cql);

      transition(ReplicationStreamState.RUNNING, null, attempt);
      completeStart();

      long lastPosition = parsePosition(options.getLsnStore().load(checkpointKey()).orElse("0"));
      while (shouldRun.get()) {
        BoundStatement bound = statement.bind(lastPosition, options.getBatchSize());
        ResultSet rs = localSession.execute(bound);
        List<ScyllaDbChangeEvent> events = new ArrayList<>();
        for (Row row : rs) {
          events.add(mapRow(row));
        }

        if (events.isEmpty()) {
          sleepInterruptibly(options.getPollIntervalMs());
          continue;
        }

        for (ScyllaDbChangeEvent event : events) {
          dispatchAndAwait(event);
          emitEventMetric(event);
          long pos = parsePosition(event.getPosition());
          if (pos > lastPosition) {
            lastPosition = pos;
            String token = Long.toString(lastPosition);
            options.getLsnStore().save(checkpointKey(), token);
            emitLsnCommitted(token);
          }
        }
      }
    } finally {
      session = null;
    }
  }

  private CqlSession openSession() {
    CqlSessionBuilder builder = CqlSession.builder()
      .addContactPoint(new InetSocketAddress(options.getHost(), options.getPort()))
      .withLocalDatacenter(options.getLocalDatacenter())
      .withKeyspace(options.getKeyspace());
    if (options.getUser() != null && !options.getUser().isBlank()) {
      builder = builder.withAuthCredentials(options.getUser(), resolvePassword());
    }
    return builder.build();
  }

  private ScyllaDbChangeEvent mapRow(Row row) {
    Map<String, Object> after = new LinkedHashMap<>();
    row.getColumnDefinitions().forEach(def -> after.put(def.getName().asInternal(), row.getObject(def.getName())));
    String rawOperation = after.getOrDefault(options.getOperationColumn(), "UPDATE").toString();
    ScyllaDbChangeEvent.Operation op = mapOperation(rawOperation);
    String position = String.valueOf(after.getOrDefault(options.getPositionColumn(), "0"));
    Map<String, Object> metadata = new LinkedHashMap<>();
    metadata.put("adapter", "scylladb");
    metadata.put("rawOperation", rawOperation);
    return new ScyllaDbChangeEvent(
      options.getKeyspace() + "." + options.getSourceTable(),
      op,
      Map.of(),
      after,
      position,
      Instant.now(),
      metadata
    );
  }

  private PreflightReport runPreflightChecks() {
    List<PreflightIssue> issues = new ArrayList<>();
    try (CqlSession localSession = openSession()) {
      TableMetadata tableMetadata = localSession.getMetadata()
        .getKeyspace(CqlIdentifier.fromCql(options.getKeyspace()))
        .flatMap(k -> k.getTable(CqlIdentifier.fromCql(options.getSourceTable())))
        .orElse(null);
      if (tableMetadata == null) {
        issues.add(new PreflightIssue(
          PreflightIssue.Severity.ERROR,
          "SOURCE_TABLE_MISSING",
          "Source table '" + options.getKeyspace() + "." + options.getSourceTable() + "' was not found",
          "Create the table and verify keyspace/sourceTable configuration."
        ));
      }
    } catch (Exception e) {
      issues.add(new PreflightIssue(
        PreflightIssue.Severity.ERROR,
        "CONNECTION_FAILED",
        "Could not connect to ScyllaDb: " + e.getMessage(),
        "Verify host, port, datacenter, keyspace, and credentials."
      ));
    }
    return new PreflightReport(issues);
  }

  private void dispatchAndAwait(ScyllaDbChangeEvent event) throws Exception {
    List<ListenerRegistration> matching = new ArrayList<>();
    for (ListenerRegistration listener : listeners) if (listener.filter.test(event)) matching.add(listener);
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

  private void invokeListener(ScyllaDbChangeEvent event,
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
    for (ReplicationMetricsListener<ScyllaDbChangeEvent> listener : metricsListeners) listener.onStateChange(change);
    for (Handler<ReplicationStateChange> handler : stateHandlers) vertx.runOnContext(v -> handler.handle(change));
  }

  private void emitEventMetric(ScyllaDbChangeEvent event) {
    for (ReplicationMetricsListener<ScyllaDbChangeEvent> listener : metricsListeners) listener.onEvent(event);
  }

  private void emitLsnCommitted(String token) {
    for (ReplicationMetricsListener<ScyllaDbChangeEvent> listener : metricsListeners) {
      listener.onLsnCommitted(checkpointKey(), token);
    }
  }

  private void failStart(Throwable err) {
    Promise<Void> promise = startPromise;
    if (promise != null && !promise.future().isComplete()) promise.fail(err);
  }

  private void completeStart() {
    Promise<Void> promise = startPromise;
    if (promise != null && !promise.future().isComplete()) promise.complete();
  }

  private String checkpointKey() {
    return "scylladb:" + options.getKeyspace() + ":" + options.getSourceTable();
  }

  private String resolvePassword() {
    String password = options.getPassword();
    if (password == null || password.isBlank()) {
      String env = options.getPasswordEnv();
      if (env != null && !env.isBlank()) password = System.getenv(env);
    }
    return password == null ? "" : password;
  }

  private static long parsePosition(String token) {
    if (token == null || token.isBlank()) return 0L;
    try {
      return Long.parseLong(token);
    } catch (Exception ignore) {
      return 0L;
    }
  }

  private static ScyllaDbChangeEvent.Operation mapOperation(String raw) {
    String normalized = raw == null ? "" : raw.toUpperCase(Locale.ROOT);
    if (normalized.startsWith("INS")) return ScyllaDbChangeEvent.Operation.INSERT;
    if (normalized.startsWith("DEL")) return ScyllaDbChangeEvent.Operation.DELETE;
    return ScyllaDbChangeEvent.Operation.UPDATE;
  }

  private static void sleepInterruptibly(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ignore) {
      Thread.currentThread().interrupt();
    }
  }

  private static final class ListenerRegistration {
    private final ChangeFilter<ScyllaDbChangeEvent> filter;
    private final ChangeConsumer<ScyllaDbChangeEvent> eventConsumer;
    private final Handler<Throwable> errorHandler;

    private ListenerRegistration(ChangeFilter<ScyllaDbChangeEvent> filter,
                                 ChangeConsumer<ScyllaDbChangeEvent> eventConsumer,
                                 Handler<Throwable> errorHandler) {
      this.filter = filter;
      this.eventConsumer = eventConsumer;
      this.errorHandler = errorHandler;
    }
  }
}
