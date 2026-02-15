package dev.henneberger.vertx.cassandra.replication;

import io.vertx.core.json.JsonObject;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class CassandraChangeEvent {

  public enum Operation {
    INSERT,
    UPDATE,
    DELETE
  }

  private final String source;
  private final Operation operation;
  private final Map<String, Object> before;
  private final Map<String, Object> after;
  private final String position;
  private final Instant commitTimestamp;
  private final Map<String, Object> metadata;

  public CassandraChangeEvent(String source,
                          Operation operation,
                          Map<String, Object> before,
                          Map<String, Object> after,
                          String position,
                          Instant commitTimestamp,
                          Map<String, Object> metadata) {
    this.source = Objects.requireNonNull(source, "source");
    this.operation = Objects.requireNonNull(operation, "operation");
    this.before = immutableCopy(before);
    this.after = immutableCopy(after);
    this.position = position;
    this.commitTimestamp = commitTimestamp;
    this.metadata = immutableCopy(metadata);
  }

  public String getSource() { return source; }
  public Operation getOperation() { return operation; }
  public Map<String, Object> getBefore() { return before; }
  public Map<String, Object> getAfter() { return after; }
  public String getPosition() { return position; }
  public Instant getCommitTimestamp() { return commitTimestamp; }
  public Map<String, Object> getMetadata() { return metadata; }

  public JsonObject afterJson() {
    return new JsonObject(after);
  }

  private static Map<String, Object> immutableCopy(Map<String, Object> input) {
    if (input == null || input.isEmpty()) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(new LinkedHashMap<>(input));
  }
}
