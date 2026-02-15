package dev.henneberger.vertx.oracle.replication;

import dev.henneberger.vertx.replication.core.ChangeFilter;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

public final class OracleChangeFilter implements ChangeFilter<OracleChangeEvent> {

  private final Set<String> includeSources = new LinkedHashSet<>();
  private final EnumSet<OracleChangeEvent.Operation> operations = EnumSet.allOf(OracleChangeEvent.Operation.class);

  private OracleChangeFilter() {
  }

  public static OracleChangeFilter all() {
    return new OracleChangeFilter();
  }

  public static OracleChangeFilter sources(String... sources) {
    return new OracleChangeFilter().includeSources(sources);
  }

  public OracleChangeFilter includeSources(String... sources) {
    Objects.requireNonNull(sources, "sources");
    for (String source : sources) {
      if (source != null && !source.isBlank()) {
        includeSources.add(source.toLowerCase(Locale.ROOT));
      }
    }
    return this;
  }

  public OracleChangeFilter operations(OracleChangeEvent.Operation... operations) {
    Objects.requireNonNull(operations, "operations");
    this.operations.clear();
    this.operations.addAll(Arrays.asList(operations));
    return this;
  }

  @Override
  public boolean test(OracleChangeEvent event) {
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
