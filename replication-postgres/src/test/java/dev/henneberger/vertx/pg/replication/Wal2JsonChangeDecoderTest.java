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

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class Wal2JsonChangeDecoderTest {

  private final ChangeDecoder decoder = new Wal2JsonChangeDecoder();

  @Test
  void decodesFormatVersion2Insert() {
    String payload = "{\"change\":[{\"kind\":\"insert\",\"schema\":\"public\",\"table\":\"users\","
      + "\"columnnames\":[\"id\",\"name\"],\"columnvalues\":[1,\"alice\"]}]}";

    List<PostgresChangeEvent> events = decoder.decode(payload, "0/16B6C50");

    assertEquals(1, events.size());
    PostgresChangeEvent event = events.get(0);
    assertEquals(PostgresChangeEvent.Operation.INSERT, event.getOperation());
    assertEquals("public.users", event.getTable());
    assertEquals(1, ((Number) event.getNewData().get("id")).intValue());
    assertEquals("alice", event.getNewData().get("name"));
    assertEquals("0/16B6C50", event.getLsn());
  }

  @Test
  void decodesLegacyFormat() {
    String payload = "{\"action\":\"U\",\"schema\":\"public\",\"table\":\"users\","
      + "\"columns\":[{\"name\":\"id\",\"value\":1},{\"name\":\"name\",\"value\":\"bob\"}]}";

    List<PostgresChangeEvent> events = decoder.decode(payload, null);

    assertEquals(1, events.size());
    PostgresChangeEvent event = events.get(0);
    assertEquals(PostgresChangeEvent.Operation.UPDATE, event.getOperation());
    assertEquals("public.users", event.getTable());
    assertEquals("bob", event.getNewData().get("name"));
  }

  @Test
  void normalizesEmbeddedJsonValues() {
    String payload = "{\"change\":[{\"kind\":\"insert\",\"schema\":\"public\",\"table\":\"events\","
      + "\"columnnames\":[\"meta\",\"tags\"],\"columnvalues\":[\"{\\\"a\\\":1}\",\"[1,2]\"]}]}";

    List<PostgresChangeEvent> events = decoder.decode(payload, null);

    assertEquals(1, events.size());
    Map<String, Object> data = events.get(0).getNewData();
    assertTrue(data.get("meta") instanceof Map);
    assertTrue(data.get("tags") instanceof List);
  }
}
