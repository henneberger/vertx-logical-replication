/*
 * Copyright (C) 2026 Daniel Henneberger
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.henneberger.vertx.pg.replication;

import dev.henneberger.vertx.replication.core.RetryPolicy;
import java.time.Duration;
import java.util.Objects;

public final class ReplicationOptionPresets {

  private ReplicationOptionPresets() {
  }

  public static void applyProductionDefaults(PostgresReplicationOptions options) {
    Objects.requireNonNull(options, "options");
    options
      .setPreflightEnabled(true)
      .setAutoStart(false)
      .setRetryPolicy(
        RetryPolicy.exponentialBackoff()
          .setInitialDelay(Duration.ofMillis(500))
          .setMaxDelay(Duration.ofSeconds(30))
          .setMultiplier(2.0d)
          .setJitter(0.2d)
      );
  }

  public static void applyLocalDevDefaults(PostgresReplicationOptions options) {
    Objects.requireNonNull(options, "options");
    options
      .setPreflightEnabled(false)
      .setAutoStart(true)
      .setRetryPolicy(
        RetryPolicy.exponentialBackoff()
          .setInitialDelay(Duration.ofMillis(200))
          .setMaxDelay(Duration.ofSeconds(5))
          .setMultiplier(1.5d)
          .setJitter(0.1d)
      );
  }
}
