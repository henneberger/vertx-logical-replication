package dev.henneberger.vertx.cassandra.replication;

import dev.henneberger.vertx.replication.core.ChangeFilter;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

public final class CassandraChangeFilter implements ChangeFilter<CassandraChangeEvent> {

  private final Set<String> includeSources = new LinkedHashSet<>();
  private final EnumSet<CassandraChangeEvent.Operation> operations = EnumSet.allOf(CassandraChangeEvent.Operation.class);

  private CassandraChangeFilter() {
  }

  public static CassandraChangeFilter all() {
    return new CassandraChangeFilter();
  }

  public static CassandraChangeFilter sources(String... sources) {
    return new CassandraChangeFilter().includeSources(sources);
  }

  public CassandraChangeFilter includeSources(String... sources) {
    Objects.requireNonNull(sources, "sources");
    for (String source : sources) {
      if (source != null && !source.isBlank()) {
        includeSources.add(source.toLowerCase(Locale.ROOT));
      }
    }
    return this;
  }

  public CassandraChangeFilter operations(CassandraChangeEvent.Operation... operations) {
    Objects.requireNonNull(operations, "operations");
    this.operations.clear();
    this.operations.addAll(Arrays.asList(operations));
    return this;
  }

  @Override
  public boolean test(CassandraChangeEvent event) {
    if (!operations.contains(event.getOperation())) {
      return false;
    }
    if (includeSources.isEmpty()) {
      return true;
    }
    return includeSources.contains(event.getSource().toLowerCase(Locale.ROOT));
  }

  public Set<String> includedSources() {
    return Collections.unmodifiableSet(includeSources);
  }
}
