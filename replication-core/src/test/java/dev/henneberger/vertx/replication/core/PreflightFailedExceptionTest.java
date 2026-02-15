package dev.henneberger.vertx.replication.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class PreflightFailedExceptionTest {

  @Test
  void carriesReportAndState() {
    PreflightReport report = new PreflightReport(List.of(
      new PreflightIssue(
        PreflightIssue.Severity.ERROR,
        "SOURCE_DOWN",
        "Cannot reach source database",
        "Check network and credentials."
      )));
    PreflightFailedException error = new PreflightFailedException(report, ReplicationStreamState.STARTING);

    assertSame(report, error.report());
    assertEquals(ReplicationStreamState.STARTING, error.state());
    assertTrue(error.getMessage().contains("SOURCE_DOWN"));
  }
}
