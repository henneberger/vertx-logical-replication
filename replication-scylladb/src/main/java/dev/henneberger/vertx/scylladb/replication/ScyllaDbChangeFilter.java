package dev.henneberger.vertx.scylladb.replication;

import dev.henneberger.vertx.replication.core.ChangeFilter;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

public final class ScyllaDbChangeFilter implements ChangeFilter<ScyllaDbChangeEvent> {

  private final Set<String> includeSources = new LinkedHashSet<>();
  private final EnumSet<ScyllaDbChangeEvent.Operation> operations = EnumSet.allOf(ScyllaDbChangeEvent.Operation.class);

  private ScyllaDbChangeFilter() {
  }

  public static ScyllaDbChangeFilter all() {
    return new ScyllaDbChangeFilter();
  }

  public static ScyllaDbChangeFilter sources(String... sources) {
    return new ScyllaDbChangeFilter().includeSources(sources);
  }

  public ScyllaDbChangeFilter includeSources(String... sources) {
    Objects.requireNonNull(sources, "sources");
    for (String source : sources) {
      if (source != null && !source.isBlank()) {
        includeSources.add(source.toLowerCase(Locale.ROOT));
      }
    }
    return this;
  }

  public ScyllaDbChangeFilter operations(ScyllaDbChangeEvent.Operation... operations) {
    Objects.requireNonNull(operations, "operations");
    this.operations.clear();
    this.operations.addAll(Arrays.asList(operations));
    return this;
  }

  @Override
  public boolean test(ScyllaDbChangeEvent event) {
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
