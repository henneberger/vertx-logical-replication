package dev.henneberger.vertx.sqlserver.replication;

import dev.henneberger.vertx.replication.core.ChangeFilter;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

public final class SqlServerChangeFilter implements ChangeFilter<SqlServerChangeEvent> {

  private final Set<String> includeInstances = new LinkedHashSet<>();
  private final EnumSet<SqlServerChangeEvent.Operation> operations =
    EnumSet.allOf(SqlServerChangeEvent.Operation.class);

  private SqlServerChangeFilter() {
  }

  public static SqlServerChangeFilter all() {
    return new SqlServerChangeFilter();
  }

  public static SqlServerChangeFilter instances(String... instances) {
    return new SqlServerChangeFilter().includeInstances(instances);
  }

  public SqlServerChangeFilter includeInstances(String... instances) {
    Objects.requireNonNull(instances, "instances");
    for (String instance : instances) {
      if (instance != null && !instance.isBlank()) {
        includeInstances.add(instance.toLowerCase(Locale.ROOT));
      }
    }
    return this;
  }

  public SqlServerChangeFilter operations(SqlServerChangeEvent.Operation... operations) {
    Objects.requireNonNull(operations, "operations");
    this.operations.clear();
    this.operations.addAll(Arrays.asList(operations));
    return this;
  }

  @Override
  public boolean test(SqlServerChangeEvent event) {
    if (!operations.contains(event.getOperation())) {
      return false;
    }
    if (includeInstances.isEmpty()) {
      return true;
    }
    return includeInstances.contains(event.getCaptureInstance().toLowerCase(Locale.ROOT));
  }

  public Set<String> includedInstances() {
    return Collections.unmodifiableSet(includeInstances);
  }
}
