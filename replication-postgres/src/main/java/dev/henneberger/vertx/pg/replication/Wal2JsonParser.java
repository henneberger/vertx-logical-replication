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

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

final class Wal2JsonParser {

  private Wal2JsonParser() {
  }

  static List<PostgresChangeEvent> parse(String message, String fallbackLsn) {
    if (message == null || message.isBlank()) {
      return Collections.emptyList();
    }

    JsonObject payload = new JsonObject(message);
    String payloadLsn = payload.getString("nextlsn", fallbackLsn);
    Instant sourceTimestamp = parseTimestamp(payload.getString("timestamp"));

    if (payload.containsKey("action")) {
      return parseLegacy(payload, payloadLsn, sourceTimestamp);
    }

    JsonArray changes = payload.getJsonArray("change");
    if (changes == null || changes.isEmpty()) {
      return Collections.emptyList();
    }

    List<PostgresChangeEvent> events = new ArrayList<>();
    for (int i = 0; i < changes.size(); i++) {
      Object raw = changes.getValue(i);
      if (!(raw instanceof JsonObject)) {
        continue;
      }

      JsonObject change = (JsonObject) raw;
      PostgresChangeEvent.Operation operation = mapOperation(change.getString("kind"));
      if (operation == null) {
        continue;
      }

      String fullTable = tableName(change.getString("schema"), change.getString("table"));
      if (fullTable == null) {
        continue;
      }

      Map<String, Object> newData = parseNewData(change);
      Map<String, Object> oldKeys = parseOldKeys(change);
      events.add(new PostgresChangeEvent(fullTable, operation, newData, oldKeys, sourceTimestamp, payloadLsn));
    }
    return events;
  }

  private static List<PostgresChangeEvent> parseLegacy(JsonObject payload,
                                                        String lsn,
                                                        Instant sourceTimestamp) {
    PostgresChangeEvent.Operation operation = mapLegacyOperation(payload.getString("action"));
    if (operation == null) {
      return Collections.emptyList();
    }

    String fullTable = tableName(payload.getString("schema"), payload.getString("table"));
    if (fullTable == null) {
      return Collections.emptyList();
    }

    Map<String, Object> data = new LinkedHashMap<>();
    JsonArray columns = payload.getJsonArray("columns");
    if (columns != null) {
      for (int i = 0; i < columns.size(); i++) {
        Object raw = columns.getValue(i);
        if (!(raw instanceof JsonObject)) {
          continue;
        }
        JsonObject column = (JsonObject) raw;
        String name = column.getString("name");
        if (name != null) {
          data.put(name, normalizeValue(column.getValue("value")));
        }
      }
    }

    Map<String, Object> newData = operation == PostgresChangeEvent.Operation.DELETE ? Collections.emptyMap() : data;
    Map<String, Object> oldKeys = operation == PostgresChangeEvent.Operation.DELETE ? data : Collections.emptyMap();
    return Collections.singletonList(new PostgresChangeEvent(fullTable, operation, newData, oldKeys, sourceTimestamp, lsn));
  }

  private static Map<String, Object> parseNewData(JsonObject change) {
    Map<String, Object> data = new LinkedHashMap<>();

    JsonArray names = change.getJsonArray("columnnames");
    JsonArray values = change.getJsonArray("columnvalues");
    if (names != null && values != null && names.size() == values.size()) {
      for (int idx = 0; idx < names.size(); idx++) {
        String name = names.getString(idx);
        Object value = values.getValue(idx);
        data.put(name, normalizeValue(value));
      }
    }

    return data;
  }

  private static Map<String, Object> parseOldKeys(JsonObject change) {
    Map<String, Object> data = new LinkedHashMap<>();

    JsonObject oldKeys = change.getJsonObject("oldkeys");
    if (oldKeys != null) {
      JsonArray oldNames = oldKeys.getJsonArray("keynames");
      JsonArray oldValues = oldKeys.getJsonArray("keyvalues");
      if (oldNames != null && oldValues != null && oldNames.size() == oldValues.size()) {
        for (int idx = 0; idx < oldNames.size(); idx++) {
          String name = oldNames.getString(idx);
          Object value = oldValues.getValue(idx);
          data.put(name, normalizeValue(value));
        }
      }
    }

    return data;
  }

  private static String tableName(String schema, String table) {
    if (table == null || table.isBlank()) {
      return null;
    }
    if (schema == null || schema.isBlank()) {
      return table;
    }
    return schema + "." + table;
  }

  private static PostgresChangeEvent.Operation mapOperation(String kind) {
    if (kind == null) {
      return null;
    }
    switch (kind.toLowerCase(Locale.ROOT)) {
      case "insert":
        return PostgresChangeEvent.Operation.INSERT;
      case "update":
        return PostgresChangeEvent.Operation.UPDATE;
      case "delete":
        return PostgresChangeEvent.Operation.DELETE;
      default:
        return null;
    }
  }

  private static PostgresChangeEvent.Operation mapLegacyOperation(String action) {
    if (action == null) {
      return null;
    }
    switch (action.toUpperCase(Locale.ROOT)) {
      case "I":
        return PostgresChangeEvent.Operation.INSERT;
      case "U":
        return PostgresChangeEvent.Operation.UPDATE;
      case "D":
        return PostgresChangeEvent.Operation.DELETE;
      default:
        return null;
    }
  }

  private static Instant parseTimestamp(String ts) {
    if (ts == null || ts.isBlank()) {
      return null;
    }
    try {
      return Instant.parse(ts);
    } catch (DateTimeParseException ignored) {
      return null;
    }
  }

  private static Object normalizeValue(Object value) {
    if (!(value instanceof String)) {
      return value;
    }

    String trimmed = ((String) value).trim();
    if ((trimmed.startsWith("{") && trimmed.endsWith("}"))
      || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
      try {
        if (trimmed.startsWith("{")) {
          return new JsonObject(trimmed).getMap();
        }
        return new JsonArray(trimmed).getList();
      } catch (RuntimeException ignored) {
        return value;
      }
    }
    return value;
  }
}
