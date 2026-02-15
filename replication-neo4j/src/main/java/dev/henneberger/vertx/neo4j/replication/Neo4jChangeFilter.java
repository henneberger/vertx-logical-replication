package dev.henneberger.vertx.neo4j.replication;

import dev.henneberger.vertx.replication.core.ChangeFilter;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

public final class Neo4jChangeFilter implements ChangeFilter<Neo4jChangeEvent> {

  private final Set<String> includeSources = new LinkedHashSet<>();
  private final EnumSet<Neo4jChangeEvent.Operation> operations = EnumSet.allOf(Neo4jChangeEvent.Operation.class);

  private Neo4jChangeFilter() {
  }

  public static Neo4jChangeFilter all() {
    return new Neo4jChangeFilter();
  }

  public static Neo4jChangeFilter sources(String... sources) {
    return new Neo4jChangeFilter().includeSources(sources);
  }

  public Neo4jChangeFilter includeSources(String... sources) {
    Objects.requireNonNull(sources, "sources");
    for (String source : sources) {
      if (source != null && !source.isBlank()) {
        includeSources.add(source.toLowerCase(Locale.ROOT));
      }
    }
    return this;
  }

  public Neo4jChangeFilter operations(Neo4jChangeEvent.Operation... operations) {
    Objects.requireNonNull(operations, "operations");
    this.operations.clear();
    this.operations.addAll(Arrays.asList(operations));
    return this;
  }

  @Override
  public boolean test(Neo4jChangeEvent event) {
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
