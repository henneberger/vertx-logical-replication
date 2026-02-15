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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ReplicationAppConfigTest {

  @Test
  void mapsEnvironmentAndBuildsOptions() {
    Map<String, String> env = new HashMap<>();
    env.put("PGHOST", "pg.internal");
    env.put("PGPORT", "15432");
    env.put("PGDATABASE", "app");
    env.put("PGUSER", "service");
    env.put("PG_PASSWORD_ENV", "APP_DB_PASSWORD");
    env.put("PGSSL", "true");
    env.put("HTTP_PORT", "9090");

    ReplicationAppConfig cfg = ReplicationAppConfig.fromMap(env, "fraud_detection_slot");

    assertEquals("pg.internal", cfg.pgHost());
    assertEquals(15432, cfg.pgPort());
    assertEquals("app", cfg.pgDatabase());
    assertEquals("service", cfg.pgUser());
    assertEquals("APP_DB_PASSWORD", cfg.pgPasswordEnv());
    assertTrue(cfg.ssl());
    assertEquals(9090, cfg.httpPort());

    PostgresReplicationOptions options = cfg.toReplicationOptions();
    assertEquals("pg.internal", options.getHost());
    assertEquals(15432, options.getPort());
    assertEquals("fraud_detection_slot", options.getSlotName());
    assertEquals("APP_DB_PASSWORD", options.getPasswordEnv());
  }
}
