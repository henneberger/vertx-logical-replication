package dev.henneberger.vertx.neo4j.replication;

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
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.Neo4jException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jLogicalReplicationStream implements ReplicationStream<Neo4jChangeEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(Neo4jLogicalReplicationStream.class);

  private final Vertx vertx;
  private final Neo4jReplicationOptions options;
  private final List<ListenerRegistration> listeners = new CopyOnWriteArrayList<>();
  private final List<Handler<ReplicationStateChange>> stateHandlers = new CopyOnWriteArrayList<>();
  private final List<ReplicationMetricsListener<Neo4jChangeEvent>> metricsListeners = new CopyOnWriteArrayList<>();
  private final AtomicBoolean shouldRun = new AtomicBoolean(false);

  private volatile Thread worker;
  private volatile Promise<Void> startPromise;
  private volatile ReplicationStreamState state = ReplicationStreamState.CREATED;
  private volatile Driver driver;

  public Neo4jLogicalReplicationStream(Vertx vertx, Neo4jReplicationOptions options) {
    this.vertx = Objects.requireNonNull(vertx, "vertx");
    this.options = new Neo4jReplicationOptions(Objects.requireNonNull(options, "options"));
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
  public ReplicationSubscription addMetricsListener(ReplicationMetricsListener<Neo4jChangeEvent> listener) {
    ReplicationMetricsListener<Neo4jChangeEvent> resolved = Objects.requireNonNull(listener, "listener");
    metricsListeners.add(resolved);
    return () -> metricsListeners.remove(resolved);
  }

  @Override
  public ReplicationSubscription subscribe(ChangeFilter<Neo4jChangeEvent> filter,
                                           ChangeConsumer<Neo4jChangeEvent> eventConsumer,
                                           Handler<Throwable> errorHandler) {
    return registerSubscription(filter, eventConsumer, errorHandler, true);
  }

  public Neo4jChangeSubscription subscribe(Neo4jChangeFilter filter,
                                           Neo4jChangeConsumer eventConsumer,
                                           Handler<Throwable> errorHandler) {
    return registerSubscription(filter, eventConsumer, errorHandler, true);
  }

  @Override
  public SubscriptionRegistration startAndSubscribe(ChangeFilter<Neo4jChangeEvent> filter,
                                                    ChangeConsumer<Neo4jChangeEvent> eventConsumer,
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
    return dev.henneberger.vertx.replication.core.AdapterMode.DB_NATIVE_CDC;
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

    Driver current = driver;
    driver = null;
    if (current != null) {
      current.close();
    }

    Promise<Void> currentStartPromise = startPromise;
    startPromise = null;
    if (currentStartPromise != null && !currentStartPromise.future().isComplete()) {
      currentStartPromise.fail("stream closed before reaching RUNNING");
    }
  }

  private Neo4jChangeSubscription registerSubscription(ChangeFilter<Neo4jChangeEvent> filter,
                                                       ChangeConsumer<Neo4jChangeEvent> eventConsumer,
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
    worker = new Thread(this::runLoop, "neo4j-cdc-" + options.getDatabase());
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
          return;
        }
        notifyError(e);
        LOG.error("Neo4j CDC stream failed for {}", options.getDatabase(), e);

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
    try (Driver localDriver = GraphDatabase.driver(options.getUri(), AuthTokens.basic(options.getUser(), resolvePassword()))) {
      driver = localDriver;
      transition(ReplicationStreamState.RUNNING, null, attempt);
      completeStart();

      long lastPosition = parsePosition(options.getLsnStore().load(checkpointKey()).orElse("0"));

      while (shouldRun.get()) {
        List<Neo4jChangeEvent> events = new ArrayList<>();
        try (Session session = localDriver.session(SessionConfig.builder().withDatabase(options.getDatabase()).build())) {
          Map<String, Object> params = new LinkedHashMap<>();
          params.put("lastPosition", lastPosition);
          params.put("limit", options.getBatchSize());
          params.put("source", options.getSourceName());
          Result result = session.run(options.getEventQuery(), params);
          while (result.hasNext()) {
            events.add(toEvent(result.next()));
          }
        }

        if (events.isEmpty()) {
          sleepInterruptibly(options.getPollIntervalMs());
          continue;
        }

        for (Neo4jChangeEvent event : events) {
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
    } finally {
      driver = null;
    }
  }

  private Neo4jChangeEvent toEvent(Record record) {
    String source = readString(record, "source", options.getSourceName());
    String rawOperation = readString(record, "operation", "UPDATE");
    Neo4jChangeEvent.Operation operation = mapOperation(rawOperation);
    Map<String, Object> before = readMap(record.get("before"));
    Map<String, Object> after = readMap(record.get("after"));
    String position = readString(record, "position", "");
    Instant commitTs = readInstant(record.get("commitTimestamp"));

    Map<String, Object> metadata = new LinkedHashMap<>();
    metadata.put("adapter", "neo4j");
    metadata.put("rawOperation", rawOperation);
    metadata.put("labels", readObject(record, "labels"));
    metadata.put("entityType", readObject(record, "entityType"));

    return new Neo4jChangeEvent(source, operation, before, after, position, commitTs, metadata);
  }

  private PreflightReport runPreflightChecks() {
    List<PreflightIssue> issues = new ArrayList<>();
    try (Driver localDriver = GraphDatabase.driver(options.getUri(), AuthTokens.basic(options.getUser(), resolvePassword()));
         Session session = localDriver.session(SessionConfig.builder().withDatabase(options.getDatabase()).build())) {
      session.run("RETURN 1").consume();
    } catch (Neo4jException e) {
      issues.add(new PreflightIssue(
        PreflightIssue.Severity.ERROR,
        "CONNECTION_FAILED",
        "Could not connect to Neo4j: " + e.getMessage(),
        "Verify uri, database, credentials, and access rights."
      ));
    }
    return new PreflightReport(issues);
  }

  private void dispatchAndAwait(Neo4jChangeEvent event) throws Exception {
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

  private void invokeListener(Neo4jChangeEvent event,
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
    for (ReplicationMetricsListener<Neo4jChangeEvent> listener : metricsListeners) {
      listener.onStateChange(change);
    }
    for (Handler<ReplicationStateChange> handler : stateHandlers) {
      vertx.runOnContext(v -> handler.handle(change));
    }
  }

  private void emitEventMetric(Neo4jChangeEvent event) {
    for (ReplicationMetricsListener<Neo4jChangeEvent> listener : metricsListeners) {
      listener.onEvent(event);
    }
  }

  private void emitLsnCommitted(String token) {
    for (ReplicationMetricsListener<Neo4jChangeEvent> listener : metricsListeners) {
      listener.onLsnCommitted(checkpointKey(), token);
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

  private String checkpointKey() {
    return "neo4j:" + options.getDatabase() + ":" + options.getSourceName();
  }

  private static long parsePosition(String token) {
    if (token == null || token.isBlank()) {
      return 0L;
    }
    try {
      return Long.parseLong(token);
    } catch (Exception ignore) {
      return 0L;
    }
  }

  private static String readString(Record record, String key, String fallback) {
    if (!record.containsKey(key) || record.get(key).isNull()) {
      return fallback;
    }
    return record.get(key).asString(fallback);
  }

  private static Object readObject(Record record, String key) {
    if (!record.containsKey(key) || record.get(key).isNull()) {
      return null;
    }
    return record.get(key).asObject();
  }

  private static Map<String, Object> readMap(Value value) {
    if (value == null || value.isNull()) {
      return Collections.emptyMap();
    }
    Object raw = value.asObject();
    if (raw instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String, Object> map = (Map<String, Object>) raw;
      return map;
    }
    return Collections.emptyMap();
  }

  private static Instant readInstant(Value value) {
    if (value == null || value.isNull()) {
      return null;
    }
    Object raw = value.asObject();
    if (raw instanceof Instant) {
      return (Instant) raw;
    }
    if (raw instanceof String) {
      try {
        return Instant.parse((String) raw);
      } catch (Exception ignore) {
        return null;
      }
    }
    return null;
  }

  private static Neo4jChangeEvent.Operation mapOperation(String raw) {
    String normalized = raw == null ? "" : raw.toUpperCase(Locale.ROOT);
    if (normalized.startsWith("INS")) {
      return Neo4jChangeEvent.Operation.INSERT;
    }
    if (normalized.startsWith("DEL")) {
      return Neo4jChangeEvent.Operation.DELETE;
    }
    return Neo4jChangeEvent.Operation.UPDATE;
  }

  private static void sleepInterruptibly(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ignore) {
      Thread.currentThread().interrupt();
    }
  }

  private static final class ListenerRegistration {
    private final ChangeFilter<Neo4jChangeEvent> filter;
    private final ChangeConsumer<Neo4jChangeEvent> eventConsumer;
    private final Handler<Throwable> errorHandler;

    private ListenerRegistration(ChangeFilter<Neo4jChangeEvent> filter,
                                 ChangeConsumer<Neo4jChangeEvent> eventConsumer,
                                 Handler<Throwable> errorHandler) {
      this.filter = filter;
      this.eventConsumer = eventConsumer;
      this.errorHandler = errorHandler;
    }
  }
}
