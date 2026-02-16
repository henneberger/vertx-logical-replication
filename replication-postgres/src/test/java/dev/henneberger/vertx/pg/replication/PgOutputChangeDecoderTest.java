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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class PgOutputChangeDecoderTest {

  @Test
  void decodesInsertWithRelationMetadata() {
    PgOutputChangeDecoder decoder = new PgOutputChangeDecoder();

    decoder.decode(beginMessage(), "0/1");
    decoder.decode(relationMessage(), "0/1");

    List<PostgresChangeEvent> events = decoder.decode(insertMessage(), "0/16B6C50");

    assertEquals(1, events.size());
    PostgresChangeEvent event = events.get(0);
    assertEquals(PostgresChangeEvent.Operation.INSERT, event.getOperation());
    assertEquals("public.users", event.getTable());
    assertEquals(1, ((Number) event.getNewData().get("id")).intValue());
    assertEquals("alice", event.getNewData().get("name"));
    assertEquals("0/16B6C50", event.getLsn());
    assertTrue(event.getSourceTimestamp() != null);
  }

  private static byte[] beginMessage() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeByte(out, 'B');
    writeLong(out, 0L);
    writeLong(out, 1_000_000L);
    writeInt(out, 42);
    return out.toByteArray();
  }

  private static byte[] relationMessage() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeByte(out, 'R');
    writeInt(out, 1);
    writeCString(out, "public");
    writeCString(out, "users");
    writeByte(out, 'd');
    writeShort(out, 2);

    writeByte(out, 0);
    writeCString(out, "id");
    writeInt(out, 23);
    writeInt(out, -1);

    writeByte(out, 0);
    writeCString(out, "name");
    writeInt(out, 25);
    writeInt(out, -1);
    return out.toByteArray();
  }

  private static byte[] insertMessage() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeByte(out, 'I');
    writeInt(out, 1);
    writeByte(out, 'N');
    writeShort(out, 2);

    writeByte(out, 't');
    writeString(out, "1");

    writeByte(out, 't');
    writeString(out, "alice");
    return out.toByteArray();
  }

  private static void writeByte(ByteArrayOutputStream out, int value) {
    out.write(value);
  }

  private static void writeShort(ByteArrayOutputStream out, int value) {
    out.write((value >>> 8) & 0xff);
    out.write(value & 0xff);
  }

  private static void writeInt(ByteArrayOutputStream out, int value) {
    out.write((value >>> 24) & 0xff);
    out.write((value >>> 16) & 0xff);
    out.write((value >>> 8) & 0xff);
    out.write(value & 0xff);
  }

  private static void writeLong(ByteArrayOutputStream out, long value) {
    out.write((int) ((value >>> 56) & 0xff));
    out.write((int) ((value >>> 48) & 0xff));
    out.write((int) ((value >>> 40) & 0xff));
    out.write((int) ((value >>> 32) & 0xff));
    out.write((int) ((value >>> 24) & 0xff));
    out.write((int) ((value >>> 16) & 0xff));
    out.write((int) ((value >>> 8) & 0xff));
    out.write((int) (value & 0xff));
  }

  private static void writeCString(ByteArrayOutputStream out, String value) {
    out.writeBytes(value.getBytes(StandardCharsets.UTF_8));
    out.write(0);
  }

  private static void writeString(ByteArrayOutputStream out, String value) {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    writeInt(out, bytes.length);
    out.writeBytes(bytes);
  }
}
