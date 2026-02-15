package dev.henneberger.vertx.cockroachdb.replication;

import dev.henneberger.vertx.replication.core.ChangeFilter;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

public final class CockroachDbChangeFilter implements ChangeFilter<CockroachDbChangeEvent> {

  private final Set<String> includeSources = new LinkedHashSet<>();
  private final EnumSet<CockroachDbChangeEvent.Operation> operations = EnumSet.allOf(CockroachDbChangeEvent.Operation.class);

  private CockroachDbChangeFilter() {
  }

  public static CockroachDbChangeFilter all() {
    return new CockroachDbChangeFilter();
  }

  public static CockroachDbChangeFilter sources(String... sources) {
    return new CockroachDbChangeFilter().includeSources(sources);
  }

  public CockroachDbChangeFilter includeSources(String... sources) {
    Objects.requireNonNull(sources, "sources");
    for (String source : sources) {
      if (source != null && !source.isBlank()) {
        includeSources.add(source.toLowerCase(Locale.ROOT));
      }
    }
    return this;
  }

  public CockroachDbChangeFilter operations(CockroachDbChangeEvent.Operation... operations) {
    Objects.requireNonNull(operations, "operations");
    this.operations.clear();
    this.operations.addAll(Arrays.asList(operations));
    return this;
  }

  @Override
  public boolean test(CockroachDbChangeEvent event) {
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
