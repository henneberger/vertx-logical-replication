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

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Decoder for the native PostgreSQL {@code pgoutput} logical replication format.
 */
public final class PgOutputChangeDecoder implements ChangeDecoder {

  private static final long PG_EPOCH_SECONDS = 946684800L;

  private static final int OID_BOOL = 16;
  private static final int OID_INT2 = 21;
  private static final int OID_INT4 = 23;
  private static final int OID_INT8 = 20;
  private static final int OID_FLOAT4 = 700;
  private static final int OID_FLOAT8 = 701;
  private static final int OID_NUMERIC = 1700;
  private static final int OID_JSON = 114;
  private static final int OID_JSONB = 3802;

  private final Map<Integer, Relation> relations = new HashMap<>();
  private volatile Instant currentTxTimestamp;

  @Override
  public List<PostgresChangeEvent> decode(String payload, String fallbackLsn) {
    return decode(payload.getBytes(StandardCharsets.ISO_8859_1), fallbackLsn);
  }

  @Override
  public synchronized List<PostgresChangeEvent> decode(byte[] payload, String fallbackLsn) {
    Cursor cursor = new Cursor(payload);
    if (!cursor.hasRemaining()) {
      return List.of();
    }

    char messageType = (char) cursor.readByte();
    switch (messageType) {
      case 'B':
        decodeBegin(cursor);
        return List.of();
      case 'C':
        decodeCommit(cursor);
        return List.of();
      case 'R':
        decodeRelation(cursor);
        return List.of();
      case 'I':
        return List.of(decodeInsert(cursor, fallbackLsn));
      case 'U':
        return List.of(decodeUpdate(cursor, fallbackLsn));
      case 'D':
        return List.of(decodeDelete(cursor, fallbackLsn));
      case 'T':
      case 'Y':
      case 'O':
      case 'M':
        return List.of();
      default:
        throw new IllegalArgumentException("Unsupported pgoutput message type: " + messageType);
    }
  }

  @Override
  public boolean supportsPlugin(String plugin) {
    return plugin != null && "pgoutput".equalsIgnoreCase(plugin);
  }

  private void decodeBegin(Cursor cursor) {
    cursor.readLong();
    long commitTimeMicros = cursor.readLong();
    currentTxTimestamp = fromPgEpochMicros(commitTimeMicros);
    cursor.readInt();
  }

  private void decodeCommit(Cursor cursor) {
    cursor.readByte();
    cursor.readLong();
    cursor.readLong();
    long commitTimeMicros = cursor.readLong();
    currentTxTimestamp = fromPgEpochMicros(commitTimeMicros);
  }

  private void decodeRelation(Cursor cursor) {
    int relationId = cursor.readInt();
    String schema = cursor.readCString();
    String table = cursor.readCString();
    cursor.readByte();
    int columnCount = cursor.readUnsignedShort();
    List<Column> columns = new ArrayList<>(columnCount);

    for (int i = 0; i < columnCount; i++) {
      cursor.readByte();
      String name = cursor.readCString();
      int typeOid = cursor.readInt();
      cursor.readInt();
      columns.add(new Column(name, typeOid));
    }

    relations.put(relationId, new Relation(schema + "." + table, columns));
  }

  private PostgresChangeEvent decodeInsert(Cursor cursor, String fallbackLsn) {
    Relation relation = relation(cursor.readInt());
    char tupleType = (char) cursor.readByte();
    if (tupleType != 'N') {
      throw new IllegalArgumentException("Unexpected tuple marker for INSERT: " + tupleType);
    }

    Map<String, Object> row = decodeTuple(cursor, relation);
    return new PostgresChangeEvent(
      relation.tableName,
      PostgresChangeEvent.Operation.INSERT,
      row,
      Map.of(),
      currentTxTimestamp,
      fallbackLsn
    );
  }

  private PostgresChangeEvent decodeUpdate(Cursor cursor, String fallbackLsn) {
    Relation relation = relation(cursor.readInt());

    Map<String, Object> oldKeys = Map.of();
    Map<String, Object> newData = Map.of();

    char marker = (char) cursor.readByte();
    if (marker == 'K' || marker == 'O') {
      oldKeys = decodeTuple(cursor, relation);
      marker = (char) cursor.readByte();
    }
    if (marker == 'N') {
      newData = decodeTuple(cursor, relation);
    }

    return new PostgresChangeEvent(
      relation.tableName,
      PostgresChangeEvent.Operation.UPDATE,
      newData,
      oldKeys,
      currentTxTimestamp,
      fallbackLsn
    );
  }

  private PostgresChangeEvent decodeDelete(Cursor cursor, String fallbackLsn) {
    Relation relation = relation(cursor.readInt());
    char marker = (char) cursor.readByte();
    if (marker != 'K' && marker != 'O') {
      throw new IllegalArgumentException("Unexpected tuple marker for DELETE: " + marker);
    }

    Map<String, Object> oldKeys = decodeTuple(cursor, relation);
    return new PostgresChangeEvent(
      relation.tableName,
      PostgresChangeEvent.Operation.DELETE,
      Map.of(),
      oldKeys,
      currentTxTimestamp,
      fallbackLsn
    );
  }

  private Map<String, Object> decodeTuple(Cursor cursor, Relation relation) {
    int colCount = cursor.readUnsignedShort();
    Map<String, Object> values = new LinkedHashMap<>();

    for (int i = 0; i < colCount; i++) {
      Column column = i < relation.columns.size()
        ? relation.columns.get(i)
        : new Column("col_" + i, 0);
      char kind = (char) cursor.readByte();
      switch (kind) {
        case 'n':
          values.put(column.name, null);
          break;
        case 'u':
          break;
        case 't': {
          int len = cursor.readInt();
          String rawValue = cursor.readString(len);
          values.put(column.name, convertTextValue(rawValue, column.typeOid));
          break;
        }
        case 'b': {
          int len = cursor.readInt();
          byte[] binary = cursor.readBytes(len);
          values.put(column.name, "base64:" + Base64.getEncoder().encodeToString(binary));
          break;
        }
        default:
          throw new IllegalArgumentException("Unsupported tuple column kind: " + kind);
      }
    }

    return values;
  }

  private static Object convertTextValue(String raw, int typeOid) {
    if (raw == null) {
      return null;
    }

    try {
      switch (typeOid) {
        case OID_BOOL:
          return "t".equalsIgnoreCase(raw) || "true".equalsIgnoreCase(raw) || "1".equals(raw);
        case OID_INT2:
        case OID_INT4:
          return Integer.parseInt(raw);
        case OID_INT8:
          return Long.parseLong(raw);
        case OID_FLOAT4:
          return Float.parseFloat(raw);
        case OID_FLOAT8:
        case OID_NUMERIC:
          return Double.parseDouble(raw);
        case OID_JSON:
        case OID_JSONB:
          return parseJson(raw);
        default:
          return raw;
      }
    } catch (RuntimeException ignored) {
      return raw;
    }
  }

  private static Object parseJson(String raw) {
    String trimmed = raw.trim();
    if (trimmed.startsWith("{")) {
      return new JsonObject(trimmed).getMap();
    }
    if (trimmed.startsWith("[")) {
      return new JsonArray(trimmed).getList();
    }
    return raw;
  }

  private Relation relation(int relationId) {
    Relation relation = relations.get(relationId);
    if (relation == null) {
      throw new IllegalArgumentException("pgoutput relation metadata missing for relation id " + relationId);
    }
    return relation;
  }

  private static Instant fromPgEpochMicros(long micros) {
    long seconds = Math.floorDiv(micros, 1_000_000L);
    long microsRemainder = Math.floorMod(micros, 1_000_000L);
    return Instant.ofEpochSecond(PG_EPOCH_SECONDS + seconds, microsRemainder * 1_000L);
  }

  private static final class Relation {
    private final String tableName;
    private final List<Column> columns;

    private Relation(String tableName, List<Column> columns) {
      this.tableName = tableName;
      this.columns = columns;
    }
  }

  private static final class Column {
    private final String name;
    private final int typeOid;

    private Column(String name, int typeOid) {
      this.name = name;
      this.typeOid = typeOid;
    }
  }

  private static final class Cursor {
    private final byte[] bytes;
    private int index;

    private Cursor(byte[] bytes) {
      this.bytes = bytes;
      this.index = 0;
    }

    private boolean hasRemaining() {
      return index < bytes.length;
    }

    private int readUnsignedShort() {
      return ((bytes[index++] & 0xff) << 8) | (bytes[index++] & 0xff);
    }

    private byte readByte() {
      return bytes[index++];
    }

    private int readInt() {
      int value = ((bytes[index] & 0xff) << 24)
        | ((bytes[index + 1] & 0xff) << 16)
        | ((bytes[index + 2] & 0xff) << 8)
        | (bytes[index + 3] & 0xff);
      index += 4;
      return value;
    }

    private long readLong() {
      long value = ((long) (bytes[index] & 0xff) << 56)
        | ((long) (bytes[index + 1] & 0xff) << 48)
        | ((long) (bytes[index + 2] & 0xff) << 40)
        | ((long) (bytes[index + 3] & 0xff) << 32)
        | ((long) (bytes[index + 4] & 0xff) << 24)
        | ((long) (bytes[index + 5] & 0xff) << 16)
        | ((long) (bytes[index + 6] & 0xff) << 8)
        | (bytes[index + 7] & 0xff);
      index += 8;
      return value;
    }

    private String readCString() {
      int start = index;
      while (index < bytes.length && bytes[index] != 0) {
        index++;
      }
      String out = new String(bytes, start, index - start, StandardCharsets.UTF_8);
      index++;
      return out;
    }

    private String readString(int len) {
      String out = new String(bytes, index, len, StandardCharsets.UTF_8);
      index += len;
      return out;
    }

    private byte[] readBytes(int len) {
      byte[] out = new byte[len];
      System.arraycopy(bytes, index, out, 0, len);
      index += len;
      return out;
    }
  }
}
