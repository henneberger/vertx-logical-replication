package dev.henneberger.vertx.mysql.replication;

import dev.henneberger.vertx.replication.core.ChangeFilter;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

public final class MySqlChangeFilter implements ChangeFilter<MySqlChangeEvent> {

  private final Set<String> includeTables = new LinkedHashSet<>();
  private final EnumSet<MySqlChangeEvent.Operation> operations =
    EnumSet.allOf(MySqlChangeEvent.Operation.class);

  private MySqlChangeFilter() {
  }

  public static MySqlChangeFilter all() {
    return new MySqlChangeFilter();
  }

  public static MySqlChangeFilter tables(String... tables) {
    return new MySqlChangeFilter().includeTables(tables);
  }

  public MySqlChangeFilter includeTables(String... tables) {
    Objects.requireNonNull(tables, "tables");
    for (String table : tables) {
      if (table != null && !table.isBlank()) {
        includeTables.add(table.toLowerCase(Locale.ROOT));
      }
    }
    return this;
  }

  public MySqlChangeFilter operations(MySqlChangeEvent.Operation... operations) {
    Objects.requireNonNull(operations, "operations");
    this.operations.clear();
    this.operations.addAll(Arrays.asList(operations));
    return this;
  }

  @Override
  public boolean test(MySqlChangeEvent event) {
    if (!operations.contains(event.getOperation())) {
      return false;
    }
    if (includeTables.isEmpty()) {
      return true;
    }
    return includeTables.contains(event.getTable().toLowerCase(Locale.ROOT));
  }

  public Set<String> includedTables() {
    return Collections.unmodifiableSet(includeTables);
  }
}
