package dev.henneberger.vertx.replication.core;

import io.vertx.core.Future;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class ReplicationStreamContractKit {

  private ReplicationStreamContractKit() {
  }

  public static void assertClosePreventsStart(ReplicationStream<?> stream, Duration timeout) {
    Objects.requireNonNull(stream, "stream");
    try {
      stream.close();
    } catch (Exception e) {
      throw new AssertionError("Unexpected failure while closing stream", e);
    }
    if (stream.state() != ReplicationStreamState.CLOSED) {
      throw new AssertionError("Expected CLOSED state after close(), got " + stream.state());
    }
    Throwable failure = awaitFailure(stream.start(), timeout);
    if (failure == null) {
      throw new AssertionError("Expected start() failure after close()");
    }
  }

  public static void assertPreflightFailureTransitionsToFailed(ReplicationStream<?> stream, Duration timeout) {
    Objects.requireNonNull(stream, "stream");
    Throwable failure = awaitFailure(stream.start(), timeout);
    if (!(failure instanceof PreflightFailedException)) {
      throw new AssertionError("Expected PreflightFailedException, got: " + failure);
    }
    if (stream.state() != ReplicationStreamState.FAILED) {
      throw new AssertionError("Expected FAILED state after preflight failure, got " + stream.state());
    }
  }

  public static Throwable awaitFailure(Future<?> future, Duration timeout) {
    Objects.requireNonNull(future, "future");
    Objects.requireNonNull(timeout, "timeout");
    try {
      future.toCompletionStage().toCompletableFuture().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
      return null;
    } catch (ExecutionException e) {
      return e.getCause();
    } catch (TimeoutException e) {
      throw new AssertionError("Timed out waiting for future completion", e);
    } catch (Exception e) {
      return e;
    }
  }
}
