package dev.henneberger.vertx.sqlserver.replication;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.henneberger.vertx.replication.core.ValueNormalizationMode;
import io.vertx.core.Vertx;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class SqlServerLogicalReplicationStreamHelpersTest {

  @Test
  void roundTripsLsnHex() {
    byte[] raw = new byte[] {0x00, 0x10, (byte) 0xFF};
    String hex = SqlServerLogicalReplicationStream.lsnToHex(raw);
    assertEquals("0x0010FF", hex);
    assertArrayEquals(raw, SqlServerLogicalReplicationStream.hexToLsn(hex));
  }

  @Test
  void comparesLsnLexicographically() {
    assertEquals(-1, SqlServerLogicalReplicationStream.compareLsn("0x0001", "0x0002"));
    assertEquals(1, SqlServerLogicalReplicationStream.compareLsn("0x0003", "0x0002"));
    assertEquals(0, SqlServerLogicalReplicationStream.compareLsn("0x0002", "0x0002"));
  }

  @Test
  void mapsSqlServerOperationCodes() {
    assertEquals(SqlServerChangeEvent.Operation.DELETE, SqlServerLogicalReplicationStream.mapOperation(1));
    assertEquals(SqlServerChangeEvent.Operation.INSERT, SqlServerLogicalReplicationStream.mapOperation(2));
    assertEquals(SqlServerChangeEvent.Operation.UPDATE, SqlServerLogicalReplicationStream.mapOperation(3));
    assertEquals(SqlServerChangeEvent.Operation.UPDATE, SqlServerLogicalReplicationStream.mapOperation(4));
    assertThrows(IllegalArgumentException.class, () -> SqlServerLogicalReplicationStream.mapOperation(99));
  }

  @Test
  void parsesAndSerializesSqlServerCdcPositionToken() {
    SqlServerCdcPosition position = new SqlServerCdcPosition("0x0011", "0x0022", 4);
    String token = position.serialize();
    SqlServerCdcPosition parsed = SqlServerCdcPosition.parse(token).orElseThrow();
    assertEquals(position, parsed);
  }

  @Test
  void acceptsLegacyLsnOnlyCheckpoint() {
    SqlServerCdcPosition parsed = SqlServerCdcPosition.parse("0x00000001").orElseThrow();
    assertEquals("0x00000001", parsed.startLsn());
    assertEquals("0x", parsed.seqVal());
    assertEquals(-1, parsed.operation());
  }

  @Test
  void comparesSqlServerCdcPositionByTupleOrdering() {
    SqlServerCdcPosition a = new SqlServerCdcPosition("0x0010", "0x0001", 2);
    SqlServerCdcPosition b = new SqlServerCdcPosition("0x0010", "0x0001", 4);
    SqlServerCdcPosition c = new SqlServerCdcPosition("0x0010", "0x0002", 1);
    SqlServerCdcPosition d = new SqlServerCdcPosition("0x0011", "0x0000", 1);
    assertTrue(a.compareTo(b) < 0);
    assertTrue(b.compareTo(c) < 0);
    assertTrue(c.compareTo(d) < 0);
  }

  @Test
  void normalizesJdbcTemporalValuesForJsonSafety() {
    Vertx vertx = Vertx.vertx();
    SqlServerLogicalReplicationStream stream = new SqlServerLogicalReplicationStream(
      vertx,
      new SqlServerReplicationOptions()
        .setHost("localhost")
        .setPort(1433)
        .setDatabase("db")
        .setUser("u")
        .setCaptureInstance("dbo_orders")
        .setValueNormalizationMode("json_safe")
    );

    try {
      assertEquals(
        "2026-02-15T20:00:00Z",
        stream.normalizeValue("created_at", Timestamp.from(Instant.parse("2026-02-15T20:00:00Z"))));
      assertEquals("2026-02-15", stream.normalizeValue("order_date", Date.valueOf("2026-02-15")));
      assertEquals("12:13:14", stream.normalizeValue("order_time", Time.valueOf("12:13:14")));
    } finally {
      stream.close();
      vertx.close();
    }
  }

  @Test
  void supportsRawNormalizationMode() {
    Timestamp ts = Timestamp.from(Instant.parse("2026-02-15T20:00:00Z"));
    Vertx vertx = Vertx.vertx();
    SqlServerLogicalReplicationStream stream = new SqlServerLogicalReplicationStream(
      vertx,
      new SqlServerReplicationOptions()
        .setHost("localhost")
        .setPort(1433)
        .setDatabase("db")
        .setUser("u")
        .setCaptureInstance("dbo_orders")
        .setValueNormalizationMode(ValueNormalizationMode.RAW.name())
    );

    try {
      assertEquals(ts, stream.normalizeValue("created_at", ts));
    } finally {
      stream.close();
      vertx.close();
    }
  }
}
