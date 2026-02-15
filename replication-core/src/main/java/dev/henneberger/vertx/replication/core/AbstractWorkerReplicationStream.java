package dev.henneberger.vertx.replication.core;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractWorkerReplicationStream<E> implements ReplicationStream<E> {

  private final Vertx vertx;
  private final List<ListenerRegistration<E>> listeners = new CopyOnWriteArrayList<>();
  private final List<Handler<ReplicationStateChange>> stateHandlers = new CopyOnWriteArrayList<>();
  private final List<ReplicationMetricsListener<E>> metricsListeners = new CopyOnWriteArrayList<>();
  private final AtomicBoolean shouldRun = new AtomicBoolean(false);

  private volatile Thread worker;
  private volatile Promise<Void> startPromise;
  private volatile ReplicationStreamState state = ReplicationStreamState.CREATED;

  protected AbstractWorkerReplicationStream(Vertx vertx) {
    this.vertx = Objects.requireNonNull(vertx, "vertx");
  }

  protected final Vertx vertx() {
    return vertx;
  }

  protected final boolean shouldRun() {
    return shouldRun.get();
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

    Future<Void> preflightFuture = preflightEnabled()
      ? preflight().compose(report -> report.ok()
      ? Future.succeededFuture()
      : Future.failedFuture(new PreflightFailedException(report, ReplicationStreamState.STARTING)))
      : Future.succeededFuture();

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
  public ReplicationSubscription addMetricsListener(ReplicationMetricsListener<E> listener) {
    ReplicationMetricsListener<E> resolved = Objects.requireNonNull(listener, "listener");
    metricsListeners.add(resolved);
    return () -> metricsListeners.remove(resolved);
  }

  @Override
  public ReplicationSubscription subscribe(ChangeFilter<E> filter,
                                           ChangeConsumer<E> eventConsumer,
                                           Handler<Throwable> errorHandler) {
    return registerSubscription(filter, eventConsumer, errorHandler, true);
  }

  @Override
  public SubscriptionRegistration startAndSubscribe(ChangeFilter<E> filter,
                                                    ChangeConsumer<E> eventConsumer,
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
  public synchronized void close() {
    shouldRun.set(false);
    transition(ReplicationStreamState.CLOSED, null, 0);

    onCloseResources();

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

  protected final ReplicationSubscription registerSubscription(ChangeFilter<E> filter,
                                                               ChangeConsumer<E> eventConsumer,
                                                               Handler<Throwable> errorHandler,
                                                               boolean withAutoStart) {
    Objects.requireNonNull(filter, "filter");
    Objects.requireNonNull(eventConsumer, "eventConsumer");
    ListenerRegistration<E> registration = new ListenerRegistration<>(filter, eventConsumer, errorHandler);
    listeners.add(registration);

    if (withAutoStart && autoStart()) {
      start().onFailure(err -> {
        if (errorHandler != null) {
          errorHandler.handle(err);
        }
      });
    }

    return () -> listeners.remove(registration);
  }

  protected final void dispatchAndAwait(E event) throws Exception {
    List<ListenerRegistration<E>> matching = new ArrayList<>();
    for (ListenerRegistration<E> listener : listeners) {
      if (listener.filter.test(event)) {
        matching.add(listener);
      }
    }
    if (matching.isEmpty()) {
      return;
    }

    int chunkSize = Math.max(1, maxConcurrentDispatch());
    for (int start = 0; start < matching.size(); start += chunkSize) {
      int end = Math.min(matching.size(), start + chunkSize);
      CountDownLatch latch = new CountDownLatch(end - start);
      AtomicReference<Throwable> failure = new AtomicReference<>();
      for (int i = start; i < end; i++) {
        ListenerRegistration<E> listener = matching.get(i);
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

  protected final void emitEventMetric(E event) {
    for (ReplicationMetricsListener<E> listener : metricsListeners) {
      listener.onEvent(event);
    }
  }

  protected final void emitLsnCommitted(String checkpointKey, String token) {
    for (ReplicationMetricsListener<E> listener : metricsListeners) {
      listener.onLsnCommitted(checkpointKey, token);
    }
  }

  protected final void emitParseFailure(String payload, Throwable error) {
    for (ReplicationMetricsListener<E> listener : metricsListeners) {
      listener.onParseFailure(payload, error);
    }
  }

  protected final void sleepInterruptibly(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ignore) {
      Thread.currentThread().interrupt();
    }
  }

  protected final void completeStart() {
    Promise<Void> promise = startPromise;
    if (promise != null && !promise.future().isComplete()) {
      promise.complete();
    }
  }

  protected final void notifyError(Throwable error) {
    for (ListenerRegistration<E> listener : listeners) {
      if (listener.errorHandler != null) {
        vertx.runOnContext(v -> listener.errorHandler.handle(error));
      }
    }
  }

  protected final void transition(ReplicationStreamState nextState, Throwable cause, long attempt) {
    ReplicationStreamState previous = state;
    if (previous == nextState && cause == null) {
      return;
    }
    state = nextState;
    ReplicationStateChange change = new ReplicationStateChange(previous, nextState, cause, attempt);
    for (ReplicationMetricsListener<E> listener : metricsListeners) {
      listener.onStateChange(change);
    }
    for (Handler<ReplicationStateChange> handler : stateHandlers) {
      vertx.runOnContext(v -> handler.handle(change));
    }
  }

  protected final String loadCheckpoint(String checkpointKey) throws Exception {
    return checkpointStore().load(checkpointKey).orElse("");
  }

  protected final void saveCheckpoint(String checkpointKey, String token) throws Exception {
    checkpointStore().save(checkpointKey, token);
  }

  private synchronized void startWorker() {
    if (!shouldRun.get()) {
      return;
    }
    if (worker != null && worker.isAlive()) {
      return;
    }

    worker = new Thread(this::runLoop, streamName());
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
        logStreamFailure(e);
        RetryPolicy retryPolicy = retryPolicy();
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

  private void failStart(Throwable err) {
    Promise<Void> promise = startPromise;
    if (promise != null && !promise.future().isComplete()) {
      promise.fail(err);
    }
  }

  private void invokeListener(E event,
                              ListenerRegistration<E> listener,
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

  protected abstract String streamName();
  protected abstract int maxConcurrentDispatch();
  protected abstract boolean preflightEnabled();
  protected abstract boolean autoStart();
  protected abstract RetryPolicy retryPolicy();
  protected abstract LsnStore checkpointStore();
  protected abstract PreflightReport runPreflightChecks();
  protected abstract void runSession(long attempt) throws Exception;
  protected abstract void logStreamFailure(Throwable error);

  protected void onCloseResources() {
    // subclasses may close active client/session resources here
  }

  private static final class ListenerRegistration<E> {
    private final ChangeFilter<E> filter;
    private final ChangeConsumer<E> eventConsumer;
    private final Handler<Throwable> errorHandler;

    private ListenerRegistration(ChangeFilter<E> filter,
                                 ChangeConsumer<E> eventConsumer,
                                 Handler<Throwable> errorHandler) {
      this.filter = filter;
      this.eventConsumer = eventConsumer;
      this.errorHandler = errorHandler;
    }
  }
}
