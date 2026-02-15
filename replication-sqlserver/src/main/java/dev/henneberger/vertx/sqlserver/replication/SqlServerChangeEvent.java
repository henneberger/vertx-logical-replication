package dev.henneberger.vertx.sqlserver.replication;

import io.vertx.core.json.JsonObject;
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

  private static Map<String, Object> copy(Map<String, Object> input) {
    if (input == null || input.isEmpty()) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(new LinkedHashMap<>(input));
  }
}
