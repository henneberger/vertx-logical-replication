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

import io.vertx.core.json.JsonObject;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A decoded change event emitted from PostgreSQL logical replication.
 */
public final class PostgresChangeEvent {

  /**
   * The row operation type.
   */
  public enum Operation {
    INSERT,
    UPDATE,
    DELETE
  }

  private final String table;
  private final Operation operation;
  private final Map<String, Object> newData;
  private final Map<String, Object> oldKeys;
  private final Instant sourceTimestamp;
  private final String lsn;

  public PostgresChangeEvent(String table,
                             Operation operation,
                             Map<String, Object> newData,
                             Map<String, Object> oldKeys,
                             Instant sourceTimestamp,
                             String lsn) {
    this.table = Objects.requireNonNull(table, "table");
    this.operation = Objects.requireNonNull(operation, "operation");
    this.newData = unmodifiableCopy(newData);
    this.oldKeys = unmodifiableCopy(oldKeys);
    this.sourceTimestamp = sourceTimestamp;
    this.lsn = lsn;
  }

  public String getTable() {
    return table;
  }

  public Operation getOperation() {
    return operation;
  }

  public Map<String, Object> getNewData() {
    return newData;
  }

  public Map<String, Object> getOldKeys() {
    return oldKeys;
  }

  public Instant getSourceTimestamp() {
    return sourceTimestamp;
  }

  public String getLsn() {
    return lsn;
  }

  public Map<String, Object> rowOrKeys() {
    return !newData.isEmpty() ? newData : oldKeys;
  }

  public String string(String field) {
    return asString(rowOrKeys().get(field));
  }

  public Integer integer(String field) {
    return asInteger(rowOrKeys().get(field));
  }

  public Long longValue(String field) {
    return asLong(rowOrKeys().get(field));
  }

  public Double doubleValue(String field) {
    return asDouble(rowOrKeys().get(field));
  }

  public BigDecimal decimal(String field) {
    return asBigDecimal(rowOrKeys().get(field));
  }

  public String stringFromNew(String field) {
    return asString(newData.get(field));
  }

  public String stringFromOld(String field) {
    return asString(oldKeys.get(field));
  }

  public JsonObject newDataJson() {
    return new JsonObject(newData);
  }

  public JsonObject oldKeysJson() {
    return new JsonObject(oldKeys);
  }

  @Override
  public String toString() {
    return "PostgresChangeEvent{" +
      "table='" + table + '\'' +
      ", operation=" + operation +
      ", newData=" + newData +
      ", oldKeys=" + oldKeys +
      ", sourceTimestamp=" + sourceTimestamp +
      ", lsn='" + lsn + '\'' +
      '}';
  }

  private static Map<String, Object> unmodifiableCopy(Map<String, Object> data) {
    if (data == null || data.isEmpty()) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(new LinkedHashMap<>(data));
  }

  private static String asString(Object value) {
    return value == null ? null : String.valueOf(value);
  }

  private static Integer asInteger(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number) {
      return ((Number) value).intValue();
    }
    if (value instanceof String) {
      try {
        return Integer.valueOf((String) value);
      } catch (NumberFormatException ignore) {
        return null;
      }
    }
    return null;
  }

  private static Long asLong(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    if (value instanceof String) {
      try {
        return Long.valueOf((String) value);
      } catch (NumberFormatException ignore) {
        return null;
      }
    }
    return null;
  }

  private static Double asDouble(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }
    if (value instanceof String) {
      try {
        return Double.valueOf((String) value);
      } catch (NumberFormatException ignore) {
        return null;
      }
    }
    return null;
  }

  private static BigDecimal asBigDecimal(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof BigDecimal) {
      return (BigDecimal) value;
    }
    if (value instanceof Number) {
      return BigDecimal.valueOf(((Number) value).doubleValue());
    }
    if (value instanceof String) {
      try {
        return new BigDecimal((String) value);
      } catch (NumberFormatException ignore) {
        return null;
      }
    }
    return null;
  }
}
