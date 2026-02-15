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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

class PostgresReplicationOptionsTest {

  @Test
  void readsFromJsonAndSerializesToJson() {
    JsonObject json = new JsonObject()
      .put("host", "db.internal")
      .put("port", 15432)
      .put("database", "app")
      .put("user", "service")
      .put("passwordEnv", "PG_PASSWORD")
      .put("ssl", true)
      .put("slotName", "app_slot")
      .put("plugin", "wal2json")
      .put("pluginOptions", new JsonObject().put("include-timestamp", true))
      .put("preflightEnabled", true)
      .put("autoStart", false)
      .put("maxConcurrentDispatch", 2)
      .put("retryPolicy", new JsonObject()
        .put("initialDelayMs", 250L)
        .put("maxDelayMs", 5000L)
        .put("multiplier", 1.5d)
        .put("jitter", 0.1d)
        .put("maxAttempts", 5L));

    PostgresReplicationOptions options = new PostgresReplicationOptions(json);

    assertEquals("db.internal", options.getHost());
    assertEquals(15432, options.getPort());
    assertEquals("app", options.getDatabase());
    assertEquals("service", options.getUser());
    assertEquals("PG_PASSWORD", options.getPasswordEnv());
    assertTrue(options.getSsl());
    assertEquals("app_slot", options.getSlotName());
    assertEquals("wal2json", options.getPlugin());
    assertTrue(options.isPreflightEnabled());
    assertFalse(options.isAutoStart());
    assertEquals(2, options.getMaxConcurrentDispatch());
    assertTrue(options.getRetryPolicy().isEnabled());
    assertEquals(5L, options.getRetryPolicy().getMaxAttempts());

    JsonObject out = options.toJson();
    assertEquals("db.internal", out.getString("host"));
    assertEquals(15432, out.getInteger("port"));
    assertFalse(out.getBoolean("autoStart"));
    assertEquals(2, out.getInteger("maxConcurrentDispatch"));
    assertTrue(out.getJsonObject("retryPolicy").containsKey("initialDelayMs"));
  }

  @Test
  void mergesWithJsonLikeOtherVertxOptions() {
    PostgresReplicationOptions base = new PostgresReplicationOptions()
      .setHost("localhost")
      .setPort(5432)
      .setDatabase("db")
      .setUser("u")
      .setSlotName("slot");

    PostgresReplicationOptions merged = base.merge(new JsonObject()
      .put("host", "replica")
      .put("ssl", true));

    assertEquals("replica", merged.getHost());
    assertTrue(merged.getSsl());
    assertEquals(5432, merged.getPort());
  }

  @Test
  void usesWal2JsonDecoderByDefault() {
    PostgresReplicationOptions options = new PostgresReplicationOptions();
    assertNotNull(options.getChangeDecoder());
    assertTrue(options.getChangeDecoder() instanceof Wal2JsonChangeDecoder);
  }

  @Test
  void failsValidationWhenDecoderDoesNotSupportPlugin() {
    ChangeDecoder unsupported = new ChangeDecoder() {
      @Override
      public java.util.List<PostgresChangeEvent> decode(String payload, String fallbackLsn) {
        return java.util.Collections.emptyList();
      }

      @Override
      public boolean supportsPlugin(String plugin) {
        return false;
      }
    };

    PostgresReplicationOptions options = new PostgresReplicationOptions()
      .setHost("localhost")
      .setPort(5432)
      .setDatabase("db")
      .setUser("u")
      .setSlotName("slot")
      .setPlugin("wal2json")
      .setChangeDecoder(unsupported);

    assertThrows(IllegalArgumentException.class, options::validate);
  }
}
