package dev.henneberger.vertx.mongodb.replication;

import dev.henneberger.vertx.replication.core.ChangeFilter;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

public final class MongoDbChangeFilter implements ChangeFilter<MongoDbChangeEvent> {

  private final Set<String> includeSources = new LinkedHashSet<>();
  private final EnumSet<MongoDbChangeEvent.Operation> operations = EnumSet.allOf(MongoDbChangeEvent.Operation.class);

  private MongoDbChangeFilter() {
  }

  public static MongoDbChangeFilter all() {
    return new MongoDbChangeFilter();
  }

  public static MongoDbChangeFilter sources(String... sources) {
    return new MongoDbChangeFilter().includeSources(sources);
  }

  public MongoDbChangeFilter includeSources(String... sources) {
    Objects.requireNonNull(sources, "sources");
    for (String source : sources) {
      if (source != null && !source.isBlank()) {
        includeSources.add(source.toLowerCase(Locale.ROOT));
      }
    }
    return this;
  }

  public MongoDbChangeFilter operations(MongoDbChangeEvent.Operation... operations) {
    Objects.requireNonNull(operations, "operations");
    this.operations.clear();
    this.operations.addAll(Arrays.asList(operations));
    return this;
  }

  @Override
  public boolean test(MongoDbChangeEvent event) {
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
