package dev.henneberger.vertx.mysql.replication;

import io.vertx.core.json.JsonObject;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class MySqlChangeEvent {

  public enum Operation {
    INSERT,
    UPDATE,
    DELETE
  }

  private final String table;
  private final Operation operation;
  private final Map<String, Object> before;
  private final Map<String, Object> after;
  private final String binlogFile;
  private final long binlogPosition;

  public MySqlChangeEvent(String table,
                          Operation operation,
                          Map<String, Object> before,
                          Map<String, Object> after,
                          String binlogFile,
                          long binlogPosition) {
    this.table = Objects.requireNonNull(table, "table");
    this.operation = Objects.requireNonNull(operation, "operation");
    this.before = copy(before);
    this.after = copy(after);
    this.binlogFile = binlogFile;
    this.binlogPosition = binlogPosition;
  }

  public String getTable() {
    return table;
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

  public String getBinlogFile() {
    return binlogFile;
  }

  public long getBinlogPosition() {
    return binlogPosition;
  }

  public JsonObject afterJson() {
    return new JsonObject(after);
  }

  private static Map<String, Object> copy(Map<String, Object> input) {
    if (input == null || input.isEmpty()) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(new LinkedHashMap<>(input));
  }
}
