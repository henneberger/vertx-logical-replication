package dev.henneberger.vertx.replication.core;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;

public final class RetryPolicy {
  private Duration initialDelay = Duration.ofSeconds(1);
  private Duration maxDelay = Duration.ofSeconds(30);
  private double multiplier = 2.0d;
  private double jitter = 0.2d;
  private long maxAttempts = 0;
  private Predicate<Throwable> retryOn = err -> true;
  private boolean enabled;

  private RetryPolicy(boolean enabled) {
    this.enabled = enabled;
  }

  public static RetryPolicy disabled() {
    return new RetryPolicy(false);
  }

  public static RetryPolicy exponentialBackoff() {
    return new RetryPolicy(true);
  }

  public RetryPolicy copy() {
    RetryPolicy copy = new RetryPolicy(enabled);
    copy.initialDelay = initialDelay;
    copy.maxDelay = maxDelay;
    copy.multiplier = multiplier;
    copy.jitter = jitter;
    copy.maxAttempts = maxAttempts;
    copy.retryOn = retryOn;
    return copy;
  }

  public RetryPolicy setInitialDelay(Duration initialDelay) {
    this.initialDelay = Objects.requireNonNull(initialDelay, "initialDelay");
    return this;
  }

  public RetryPolicy setMaxDelay(Duration maxDelay) {
    this.maxDelay = Objects.requireNonNull(maxDelay, "maxDelay");
    return this;
  }

  public RetryPolicy setMultiplier(double multiplier) {
    this.multiplier = multiplier;
    return this;
  }

  public RetryPolicy setJitter(double jitter) {
    if (jitter < 0.0d || jitter > 1.0d) {
      throw new IllegalArgumentException("jitter must be between 0.0 and 1.0");
    }
    this.jitter = jitter;
    return this;
  }

  public RetryPolicy setMaxAttempts(long maxAttempts) {
    this.maxAttempts = maxAttempts;
    return this;
  }

  public RetryPolicy setRetryOn(Predicate<Throwable> retryOn) {
    this.retryOn = Objects.requireNonNull(retryOn, "retryOn");
    return this;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public Duration getInitialDelay() {
    return initialDelay;
  }

  public Duration getMaxDelay() {
    return maxDelay;
  }

  public double getMultiplier() {
    return multiplier;
  }

  public double getJitter() {
    return jitter;
  }

  public long getMaxAttempts() {
    return maxAttempts;
  }

  public boolean shouldRetry(Throwable error, long attempt) {
    if (!enabled || !retryOn.test(error)) {
      return false;
    }
    return maxAttempts == 0 || attempt <= maxAttempts;
  }

  public long computeDelayMillis(long attempt) {
    double base = initialDelay.toMillis() * Math.pow(Math.max(1.0d, multiplier), Math.max(0, attempt - 1));
    long capped = Math.min((long) base, maxDelay.toMillis());
    if (jitter == 0.0d) {
      return capped;
    }
    long delta = (long) (capped * jitter);
    long min = Math.max(0L, capped - delta);
    long max = capped + delta;
    return ThreadLocalRandom.current().nextLong(min, max + 1);
  }

  public void validate() {
    if (initialDelay.isNegative()) {
      throw new IllegalArgumentException("initialDelay must be >= 0");
    }
    if (maxDelay.isNegative()) {
      throw new IllegalArgumentException("maxDelay must be >= 0");
    }
    if (maxDelay.compareTo(initialDelay) < 0) {
      throw new IllegalArgumentException("maxDelay must be >= initialDelay");
    }
    if (multiplier < 1.0d) {
      throw new IllegalArgumentException("multiplier must be >= 1.0");
    }
    if (maxAttempts < 0) {
      throw new IllegalArgumentException("maxAttempts must be >= 0");
    }
  }
}
