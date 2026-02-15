package dev.henneberger.vertx.sqlserver.replication;

import io.vertx.core.json.JsonObject;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class SqlServerChangeEvent {

  public enum Operation {
    INSERT,
    UPDATE,
    DELETE
  }

  private final String captureInstance;
  private final Operation operation;
  private final Map<String, Object> before;
  private final Map<String, Object> after;
  private final String commitLsn;
  private final Instant commitTimestamp;
  private final Map<String, Object> metadata;

  public SqlServerChangeEvent(String captureInstance,
                              Operation operation,
                              Map<String, Object> before,
                              Map<String, Object> after,
                              String commitLsn,
                              Instant commitTimestamp,
                              Map<String, Object> metadata) {
    this.captureInstance = Objects.requireNonNull(captureInstance, "captureInstance");
    this.operation = Objects.requireNonNull(operation, "operation");
    this.before = copy(before);
    this.after = copy(after);
    this.commitLsn = commitLsn;
    this.commitTimestamp = commitTimestamp;
    this.metadata = copy(metadata);
  }

  public String getCaptureInstance() {
    return captureInstance;
  }

  public Operation getOperation() {
    return operation;
  }

  public Map<String, Object> getBefore() {
    return before;
  }

  public Map<String, Object> getAfter() {
    return after;
  }

  public String getCommitLsn() {
    return commitLsn;
  }

  public Instant getCommitTimestamp() {
    return commitTimestamp;
  }

  public Map<String, Object> getMetadata() {
    return metadata;
  }

  public JsonObject afterJson() {
    return new JsonObject(after);
  }

  public JsonObject beforeJson() {
    return new JsonObject(before);
  }

  public Map<String, Object> rowOrBefore() {
    return !after.isEmpty() ? after : before;
  }

  public String string(String field) {
    return asString(rowOrBefore().get(field));
  }

  public Integer integer(String field) {
    return asInteger(rowOrBefore().get(field));
  }

  public Long longValue(String field) {
    return asLong(rowOrBefore().get(field));
  }

  public Double doubleValue(String field) {
    return asDouble(rowOrBefore().get(field));
  }

  public BigDecimal decimal(String field) {
    return asBigDecimal(rowOrBefore().get(field));
  }

  public String stringFromAfter(String field) {
    return asString(after.get(field));
  }

  public String stringFromBefore(String field) {
    return asString(before.get(field));
  }

  private static Map<String, Object> copy(Map<String, Object> input) {
    if (input == null || input.isEmpty()) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(new LinkedHashMap<>(input));
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
