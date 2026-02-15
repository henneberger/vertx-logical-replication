package dev.henneberger.vertx.mariadb.replication;

import dev.henneberger.vertx.replication.core.ChangeFilter;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

public final class MariaDbChangeFilter implements ChangeFilter<MariaDbChangeEvent> {

  private final Set<String> includeSources = new LinkedHashSet<>();
  private final EnumSet<MariaDbChangeEvent.Operation> operations = EnumSet.allOf(MariaDbChangeEvent.Operation.class);

  private MariaDbChangeFilter() {
  }

  public static MariaDbChangeFilter all() {
    return new MariaDbChangeFilter();
  }

  public static MariaDbChangeFilter sources(String... sources) {
    return new MariaDbChangeFilter().includeSources(sources);
  }

  public MariaDbChangeFilter includeSources(String... sources) {
    Objects.requireNonNull(sources, "sources");
    for (String source : sources) {
      if (source != null && !source.isBlank()) {
        includeSources.add(source.toLowerCase(Locale.ROOT));
      }
    }
    return this;
  }

  public MariaDbChangeFilter operations(MariaDbChangeEvent.Operation... operations) {
    Objects.requireNonNull(operations, "operations");
    this.operations.clear();
    this.operations.addAll(Arrays.asList(operations));
    return this;
  }

  @Override
  public boolean test(MariaDbChangeEvent event) {
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
