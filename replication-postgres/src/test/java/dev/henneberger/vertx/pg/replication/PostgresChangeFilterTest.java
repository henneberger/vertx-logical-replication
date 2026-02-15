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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import org.junit.jupiter.api.Test;

class PostgresChangeFilterTest {

  @Test
  void supportsOperationPresets() {
    PostgresChangeEvent insertEvent = new PostgresChangeEvent(
      "public.users",
      PostgresChangeEvent.Operation.INSERT,
      Collections.singletonMap("id", 1),
      Collections.emptyMap(),
      null,
      null
    );
    PostgresChangeEvent deleteEvent = new PostgresChangeEvent(
      "public.users",
      PostgresChangeEvent.Operation.DELETE,
      Collections.emptyMap(),
      Collections.singletonMap("id", 1),
      null,
      null
    );

    assertTrue(PostgresChangeFilter.all().onlyInserts().test(insertEvent));
    assertFalse(PostgresChangeFilter.all().onlyInserts().test(deleteEvent));
    assertTrue(PostgresChangeFilter.all().nonDeletes().test(insertEvent));
    assertFalse(PostgresChangeFilter.all().nonDeletes().test(deleteEvent));
  }
}
