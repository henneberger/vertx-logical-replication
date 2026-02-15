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

import java.util.Objects;
import org.slf4j.Logger;

public final class ReplicationLogging {

  private ReplicationLogging() {
  }

  public static PostgresChangeSubscription attachDefaultLogging(PostgresLogicalReplicationStream stream,
                                                                Logger logger,
                                                                String streamName) {
    Objects.requireNonNull(stream, "stream");
    Objects.requireNonNull(logger, "logger");
    String name = streamName == null || streamName.isBlank() ? "replication" : streamName;

    return stream.onStateChange(change -> {
      Throwable cause = change.cause();
      if (cause != null) {
        logger.warn("stream={} state={} prev={} attempt={} cause={}",
          name,
          change.state(),
          change.previousState(),
          change.attempt(),
          cause.toString());
      } else {
        logger.info("stream={} state={} prev={} attempt={}",
          name,
          change.state(),
          change.previousState(),
          change.attempt());
      }
    });
  }
}
