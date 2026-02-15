package dev.henneberger.vertx.sqlserver.replication;

import dev.henneberger.vertx.replication.core.RetryPolicy;
import dev.henneberger.vertx.replication.core.ValueNormalizationMode;
import java.time.Duration;
import java.util.Objects;

public final class SqlServerReplicationOptionPresets {

  private SqlServerReplicationOptionPresets() {
  }

  public static void applyProductionDefaults(SqlServerReplicationOptions options) {
    Objects.requireNonNull(options, "options");
    options
      .setPreflightEnabled(true)
      .setPreflightMode("wait-until-ready")
      .setPreflightMaxWaitMs(Duration.ofMinutes(1).toMillis())
      .setPreflightRetryIntervalMs(Duration.ofSeconds(2).toMillis())
      .setAutoStart(false)
      .setValueNormalizationMode(ValueNormalizationMode.JSON_SAFE.name())
      .setRetryPolicy(
        RetryPolicy.exponentialBackoff()
          .setInitialDelay(Duration.ofMillis(500))
          .setMaxDelay(Duration.ofSeconds(30))
          .setMultiplier(2.0d)
          .setJitter(0.2d)
      );
  }

  public static void applyLocalDevDefaults(SqlServerReplicationOptions options) {
    Objects.requireNonNull(options, "options");
    options
      .setPreflightEnabled(false)
      .setPreflightMode("strict")
      .setPreflightMaxWaitMs(0L)
      .setPreflightRetryIntervalMs(Duration.ofMillis(250).toMillis())
      .setAutoStart(true)
      .setValueNormalizationMode(ValueNormalizationMode.RAW.name())
      .setRetryPolicy(
        RetryPolicy.exponentialBackoff()
          .setInitialDelay(Duration.ofMillis(200))
          .setMaxDelay(Duration.ofSeconds(5))
          .setMultiplier(1.5d)
          .setJitter(0.1d)
      );
  }
}
