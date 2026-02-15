package dev.henneberger.vertx.sqlserver.replication;

import java.util.Objects;
import java.util.Optional;

public final class SqlServerCdcPosition {

  private static final String SERIALIZATION_PREFIX = "v1|";
  private static final String SEPARATOR = "|";

  private final String startLsn;
  private final String seqVal;
  private final int operation;

  public SqlServerCdcPosition(String startLsn, String seqVal, int operation) {
    this.startLsn = normalizeHex(startLsn);
    this.seqVal = normalizeHex(seqVal);
    this.operation = operation;
  }

  public static SqlServerCdcPosition initial(String minLsn) {
    return new SqlServerCdcPosition(minLsn, "0x", -1);
  }

  public static Optional<SqlServerCdcPosition> parse(String persisted) {
    if (persisted == null || persisted.isBlank()) {
      return Optional.empty();
    }
    if (!persisted.startsWith(SERIALIZATION_PREFIX)) {
      // Backward-compatible migration path from legacy LSN-only checkpoints.
      return Optional.of(new SqlServerCdcPosition(persisted, "0x", -1));
    }

    String[] parts = persisted.split("\\|", 4);
    if (parts.length != 4 || !"v1".equals(parts[0])) {
      throw new IllegalArgumentException("Invalid SQL Server CDC checkpoint token: " + persisted);
    }
    return Optional.of(new SqlServerCdcPosition(parts[1], parts[2], Integer.parseInt(parts[3])));
  }

  public String serialize() {
    return SERIALIZATION_PREFIX + startLsn + SEPARATOR + seqVal + SEPARATOR + operation;
  }

  public String startLsn() {
    return startLsn;
  }

  public String seqVal() {
    return seqVal;
  }

  public int operation() {
    return operation;
  }

  public int compareTo(SqlServerCdcPosition other) {
    int lsnCmp = SqlServerLogicalReplicationStream.compareLsn(startLsn, other.startLsn);
    if (lsnCmp != 0) {
      return lsnCmp;
    }
    int seqCmp = SqlServerLogicalReplicationStream.compareLsn(seqVal, other.seqVal);
    if (seqCmp != 0) {
      return seqCmp;
    }
    return Integer.compare(operation, other.operation);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SqlServerCdcPosition)) {
      return false;
    }
    SqlServerCdcPosition that = (SqlServerCdcPosition) o;
    return operation == that.operation
      && Objects.equals(startLsn, that.startLsn)
      && Objects.equals(seqVal, that.seqVal);
  }

  @Override
  public int hashCode() {
    return Objects.hash(startLsn, seqVal, operation);
  }

  @Override
  public String toString() {
    return "SqlServerCdcPosition{"
      + "startLsn='" + startLsn + '\''
      + ", seqVal='" + seqVal + '\''
      + ", operation=" + operation
      + '}';
  }

  private static String normalizeHex(String hex) {
    String value = hex == null ? "0x" : hex.trim();
    if (value.isEmpty()) {
      return "0x";
    }
    return value.startsWith("0x") || value.startsWith("0X") ? value : "0x" + value;
  }
}
