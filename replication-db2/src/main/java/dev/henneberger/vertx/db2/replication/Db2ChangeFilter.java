package dev.henneberger.vertx.db2.replication;

import dev.henneberger.vertx.replication.core.ChangeFilter;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

public final class Db2ChangeFilter implements ChangeFilter<Db2ChangeEvent> {

  private final Set<String> includeSources = new LinkedHashSet<>();
  private final EnumSet<Db2ChangeEvent.Operation> operations = EnumSet.allOf(Db2ChangeEvent.Operation.class);

  private Db2ChangeFilter() {
  }

  public static Db2ChangeFilter all() {
    return new Db2ChangeFilter();
  }

  public static Db2ChangeFilter sources(String... sources) {
    return new Db2ChangeFilter().includeSources(sources);
  }

  public Db2ChangeFilter includeSources(String... sources) {
    Objects.requireNonNull(sources, "sources");
    for (String source : sources) {
      if (source != null && !source.isBlank()) {
        includeSources.add(source.toLowerCase(Locale.ROOT));
      }
    }
    return this;
  }

  public Db2ChangeFilter operations(Db2ChangeEvent.Operation... operations) {
    Objects.requireNonNull(operations, "operations");
    this.operations.clear();
    this.operations.addAll(Arrays.asList(operations));
    return this;
  }

  @Override
  public boolean test(Db2ChangeEvent event) {
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
