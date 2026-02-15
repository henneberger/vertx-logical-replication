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
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PostgresChangeEventHelpersTest {

  @Test
  void readsTypedFieldsFromNewData() {
    Map<String, Object> newData = new LinkedHashMap<>();
    newData.put("id", "42");
    newData.put("amount", "19.95");
    newData.put("score", 7);
    newData.put("country", "US");

    PostgresChangeEvent event = new PostgresChangeEvent(
      "public.transactions",
      PostgresChangeEvent.Operation.INSERT,
      newData,
      Collections.emptyMap(),
      Instant.now(),
      "0/16B6C50"
    );

    assertEquals("US", event.string("country"));
    assertEquals(Integer.valueOf(42), event.integer("id"));
    assertEquals(Long.valueOf(7L), event.longValue("score"));
    assertEquals(Double.valueOf(19.95d), event.doubleValue("amount"));
    assertEquals(new BigDecimal("19.95"), event.decimal("amount"));
    assertEquals("US", event.stringFromNew("country"));
    assertNull(event.stringFromOld("country"));
    assertEquals("US", event.newDataJson().getString("country"));
  }

  @Test
  void fallsBackToOldKeysWhenNewDataIsEmpty() {
    Map<String, Object> oldKeys = new LinkedHashMap<>();
    oldKeys.put("id", 12);

    PostgresChangeEvent event = new PostgresChangeEvent(
      "public.users",
      PostgresChangeEvent.Operation.DELETE,
      Collections.emptyMap(),
      oldKeys,
      null,
      null
    );

    assertEquals(Integer.valueOf(12), event.integer("id"));
    assertEquals(12, event.rowOrKeys().get("id"));
    assertEquals(12, event.oldKeysJson().getInteger("id"));
  }
}
