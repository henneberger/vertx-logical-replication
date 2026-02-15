/*
 * Copyright (C) 2026 Daniel Henneberger
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.henneberger.vertx.pg.replication;

import dev.henneberger.vertx.replication.core.ChangeFilter;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

public final class PostgresChangeFilter implements ChangeFilter<PostgresChangeEvent> {

  private final Set<String> includeTables = new LinkedHashSet<>();
  private final Set<String> excludeTables = new LinkedHashSet<>();
  private final EnumSet<PostgresChangeEvent.Operation> operations =
    EnumSet.allOf(PostgresChangeEvent.Operation.class);

  private PostgresChangeFilter() {
  }

  public static PostgresChangeFilter all() {
    return new PostgresChangeFilter();
  }

  public static PostgresChangeFilter tables(String... tables) {
    return new PostgresChangeFilter().includeTables(tables);
  }

  public PostgresChangeFilter includeTables(String... tables) {
    addTables(includeTables, tables);
    return this;
  }

  public PostgresChangeFilter excludeTables(String... tables) {
    addTables(excludeTables, tables);
    return this;
  }

  public PostgresChangeFilter operations(PostgresChangeEvent.Operation... operations) {
    Objects.requireNonNull(operations, "operations");
    this.operations.clear();
    this.operations.addAll(Arrays.asList(operations));
    return this;
  }

  public PostgresChangeFilter only(PostgresChangeEvent.Operation... operations) {
    return operations(operations);
  }

  public PostgresChangeFilter onlyInserts() {
    return operations(PostgresChangeEvent.Operation.INSERT);
  }

  public PostgresChangeFilter nonDeletes() {
    return operations(PostgresChangeEvent.Operation.INSERT, PostgresChangeEvent.Operation.UPDATE);
  }

  @Override
  public boolean test(PostgresChangeEvent event) {
    Objects.requireNonNull(event, "event");

    if (!operations.contains(event.getOperation())) {
      return false;
    }

    String table = normalize(event.getTable());
    if (!includeTables.isEmpty() && !includeTables.contains(table)) {
      return false;
    }
    return !excludeTables.contains(table);
  }

  public Set<String> includedTables() {
    return Collections.unmodifiableSet(includeTables);
  }

  public Set<String> excludedTables() {
    return Collections.unmodifiableSet(excludeTables);
  }

  private static void addTables(Set<String> target, String... tables) {
    Objects.requireNonNull(tables, "tables");
    for (String table : tables) {
      if (table != null && !table.isBlank()) {
        target.add(normalize(table));
      }
    }
  }

  private static String normalize(String table) {
    return table.toLowerCase(Locale.ROOT);
  }
}
