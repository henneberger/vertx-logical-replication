package dev.henneberger.vertx.replication.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

class PreflightReportsTest {

  @Test
  void formatsDetailedFailureMessage() {
    PreflightReport report = new PreflightReport(List.of(
      new PreflightIssue(
        PreflightIssue.Severity.ERROR,
        "CAPTURE_INSTANCE_MISSING",
        "Capture instance 'dbo_orders' was not found",
        "Enable CDC on the table and configure captureInstance correctly."
      )));

    assertEquals(
      "Preflight failed: [CAPTURE_INSTANCE_MISSING] Capture instance 'dbo_orders' was not found "
        + "Remediation: Enable CDC on the table and configure captureInstance correctly.",
      PreflightReports.describeFailure(report)
    );
  }
}
