package dev.henneberger.vertx.mongodb.replication;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.BsonTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDbLogicalReplicationStream implements ReplicationStream<MongoDbChangeEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDbLogicalReplicationStream.class);

  private final Vertx vertx;
  private final MongoDbReplicationOptions options;
  private final List<ListenerRegistration> listeners = new CopyOnWriteArrayList<>();
  private final List<Handler<ReplicationStateChange>> stateHandlers = new CopyOnWriteArrayList<>();
  private final List<ReplicationMetricsListener<MongoDbChangeEvent>> metricsListeners = new CopyOnWriteArrayList<>();
  private final AtomicBoolean shouldRun = new AtomicBoolean(false);

  private volatile Thread worker;
  private volatile Promise<Void> startPromise;
  private volatile ReplicationStreamState state = ReplicationStreamState.CREATED;
  private volatile MongoClient mongoClient;

  public MongoDbLogicalReplicationStream(Vertx vertx, MongoDbReplicationOptions options) {
    this.vertx = Objects.requireNonNull(vertx, "vertx");
    this.options = new MongoDbReplicationOptions(Objects.requireNonNull(options, "options"));
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
  public ReplicationSubscription addMetricsListener(ReplicationMetricsListener<MongoDbChangeEvent> listener) {
    ReplicationMetricsListener<MongoDbChangeEvent> resolved = Objects.requireNonNull(listener, "listener");
    metricsListeners.add(resolved);
    return () -> metricsListeners.remove(resolved);
  }

  @Override
  public ReplicationSubscription subscribe(ChangeFilter<MongoDbChangeEvent> filter,
                                           ChangeConsumer<MongoDbChangeEvent> eventConsumer,
                                           Handler<Throwable> errorHandler) {
    return registerSubscription(filter, eventConsumer, errorHandler, true);
  }

  public MongoDbChangeSubscription subscribe(MongoDbChangeFilter filter,
                                             MongoDbChangeConsumer eventConsumer,
                                             Handler<Throwable> errorHandler) {
    return registerSubscription(filter, eventConsumer, errorHandler, true);
  }

  @Override
  public SubscriptionRegistration startAndSubscribe(ChangeFilter<MongoDbChangeEvent> filter,
                                                    ChangeConsumer<MongoDbChangeEvent> eventConsumer,
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

    Thread thread = worker;
    worker = null;
    if (thread != null) {
      thread.interrupt();
    }

    MongoClient client = mongoClient;
    mongoClient = null;
    if (client != null) {
      client.close();
    }

    Promise<Void> currentStartPromise = startPromise;
    startPromise = null;
    if (currentStartPromise != null && !currentStartPromise.future().isComplete()) {
      currentStartPromise.fail("stream closed before reaching RUNNING");
    }
  }

  private MongoDbChangeSubscription registerSubscription(ChangeFilter<MongoDbChangeEvent> filter,
                                                         ChangeConsumer<MongoDbChangeEvent> eventConsumer,
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
    worker = new Thread(this::runLoop, "mongodb-cdc-" + options.getCollection());
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
        LOG.error("MongoDB CDC stream failed for {}.{}", options.getDatabase(), options.getCollection(), e);

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
    try (MongoClient client = MongoClients.create(options.getConnectionString())) {
      mongoClient = client;
      MongoDatabase database = client.getDatabase(options.getDatabase());
      MongoCollection<Document> collection = database.getCollection(options.getCollection());
      ChangeStreamIterable<Document> iterable = collection.watch().batchSize(options.getBatchSize());
      if (options.isFullDocumentLookup()) {
        iterable.fullDocument(FullDocument.UPDATE_LOOKUP);
      }

      String checkpoint = options.getLsnStore().load(checkpointKey()).orElse("");
      if (!checkpoint.isBlank()) {
        iterable.resumeAfter(BsonDocument.parse(checkpoint));
      }

      transition(ReplicationStreamState.RUNNING, null, attempt);
      completeStart();

      try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = iterable.cursor()) {
        while (shouldRun.get() && cursor.hasNext()) {
          ChangeStreamDocument<Document> change = cursor.next();
          MongoDbChangeEvent event = toEvent(change);
          dispatchAndAwait(event);
          emitEventMetric(event);

          String token = change.getResumeToken() == null ? "" : change.getResumeToken().toJson();
          if (!token.isBlank()) {
            options.getLsnStore().save(checkpointKey(), token);
            emitLsnCommitted(token);
          }
        }
      }
    } finally {
      mongoClient = null;
    }
  }

  private MongoDbChangeEvent toEvent(ChangeStreamDocument<Document> change) {
    String rawOperation = change.getOperationType() == null ? "" : change.getOperationType().getValue();
    MongoDbChangeEvent.Operation operation = mapOperation(rawOperation);
    Map<String, Object> before = change.getFullDocumentBeforeChange() == null
      ? Collections.emptyMap()
      : change.getFullDocumentBeforeChange();
    Map<String, Object> after = change.getFullDocument() == null
      ? Collections.emptyMap()
      : change.getFullDocument();
    String position = change.getResumeToken() == null ? "" : change.getResumeToken().toJson();
    Instant commitTs = toInstant(change.getClusterTime());

    Map<String, Object> metadata = new LinkedHashMap<>();
    metadata.put("adapter", "mongodb");
    metadata.put("rawOperation", rawOperation);
    if (change.getDocumentKey() != null) {
      metadata.put("documentKey", change.getDocumentKey());
    }
    if (change.getNamespace() != null) {
      metadata.put("namespace", change.getNamespace().getFullName());
    }

    return new MongoDbChangeEvent(
      options.getDatabase() + "." + options.getCollection(),
      operation,
      before,
      after,
      position,
      commitTs,
      metadata
    );
  }

  private PreflightReport runPreflightChecks() {
    List<PreflightIssue> issues = new ArrayList<>();
    try (MongoClient client = MongoClients.create(options.getConnectionString())) {
      client.getDatabase(options.getDatabase()).runCommand(new Document("ping", 1));
      client.getDatabase(options.getDatabase()).getCollection(options.getCollection()).estimatedDocumentCount();
    } catch (Exception e) {
      issues.add(new PreflightIssue(
        PreflightIssue.Severity.ERROR,
        "CONNECTION_FAILED",
        "Could not connect to MongoDB change stream source: " + e.getMessage(),
        "Verify connectionString, database, collection, and privileges."
      ));
    }
    return new PreflightReport(issues);
  }

  private void dispatchAndAwait(MongoDbChangeEvent event) throws Exception {
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

  private void invokeListener(MongoDbChangeEvent event,
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
    for (ReplicationMetricsListener<MongoDbChangeEvent> listener : metricsListeners) {
      listener.onStateChange(change);
    }
    for (Handler<ReplicationStateChange> handler : stateHandlers) {
      vertx.runOnContext(v -> handler.handle(change));
    }
  }

  private void emitEventMetric(MongoDbChangeEvent event) {
    for (ReplicationMetricsListener<MongoDbChangeEvent> listener : metricsListeners) {
      listener.onEvent(event);
    }
  }

  private void emitLsnCommitted(String token) {
    for (ReplicationMetricsListener<MongoDbChangeEvent> listener : metricsListeners) {
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

  private String checkpointKey() {
    return "mongodb:" + options.getDatabase() + ":" + options.getCollection();
  }

  private static MongoDbChangeEvent.Operation mapOperation(String raw) {
    if ("insert".equalsIgnoreCase(raw)) {
      return MongoDbChangeEvent.Operation.INSERT;
    }
    if ("delete".equalsIgnoreCase(raw)) {
      return MongoDbChangeEvent.Operation.DELETE;
    }
    return MongoDbChangeEvent.Operation.UPDATE;
  }

  private static Instant toInstant(BsonTimestamp ts) {
    return ts == null ? null : Instant.ofEpochSecond(ts.getTime());
  }

  private static void sleepInterruptibly(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ignore) {
      Thread.currentThread().interrupt();
    }
  }

  private static final class ListenerRegistration {
    private final ChangeFilter<MongoDbChangeEvent> filter;
    private final ChangeConsumer<MongoDbChangeEvent> eventConsumer;
    private final Handler<Throwable> errorHandler;

    private ListenerRegistration(ChangeFilter<MongoDbChangeEvent> filter,
                                 ChangeConsumer<MongoDbChangeEvent> eventConsumer,
                                 Handler<Throwable> errorHandler) {
      this.filter = filter;
      this.eventConsumer = eventConsumer;
      this.errorHandler = errorHandler;
    }
  }
}
