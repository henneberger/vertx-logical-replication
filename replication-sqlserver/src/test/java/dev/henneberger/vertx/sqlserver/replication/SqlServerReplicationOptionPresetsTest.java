package dev.henneberger.vertx.sqlserver.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SqlServerReplicationOptionPresetsTest {

  @Test
  void appliesProductionDefaults() {
    SqlServerReplicationOptions options = new SqlServerReplicationOptions();
    SqlServerReplicationOptionPresets.applyProductionDefaults(options);

    assertTrue(options.isPreflightEnabled());
    assertFalse(options.isAutoStart());
    assertEquals("wait-until-ready", options.getPreflightMode());
    assertEquals("JSON_SAFE", options.getValueNormalizationMode());
    assertTrue(options.getRetryPolicy().isEnabled());
  }

  @Test
  void appliesLocalDevDefaults() {
    SqlServerReplicationOptions options = new SqlServerReplicationOptions();
    SqlServerReplicationOptionPresets.applyLocalDevDefaults(options);

    assertFalse(options.isPreflightEnabled());
    assertTrue(options.isAutoStart());
    assertEquals("strict", options.getPreflightMode());
    assertEquals("RAW", options.getValueNormalizationMode());
    assertTrue(options.getRetryPolicy().isEnabled());
  }
}
