package dev.henneberger.vertx.mysql.replication;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class MySqlLogicalReplicationStreamHelpersTest {

  @Test
  void formatsAndParsesCheckpoint() {
    String checkpoint = MySqlLogicalReplicationStream.formatCheckpoint("mysql-bin.000123", 456L);
    assertEquals("mysql-bin.000123:456", checkpoint);
    assertArrayEquals(new String[] {"mysql-bin.000123", "456"},
      MySqlLogicalReplicationStream.parseCheckpoint(checkpoint));
  }

  @Test
  void handlesEmptyCheckpoint() {
    assertArrayEquals(new String[] {"", "4"},
      MySqlLogicalReplicationStream.parseCheckpoint(""));
  }
}
