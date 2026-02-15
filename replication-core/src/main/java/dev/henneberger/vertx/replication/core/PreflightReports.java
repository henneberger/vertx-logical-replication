package dev.henneberger.vertx.replication.core;

import java.util.Objects;
import java.util.stream.Collectors;

public final class PreflightReports {

  private PreflightReports() {
  }

  public static String describeFailure(PreflightReport report) {
    Objects.requireNonNull(report, "report");
    if (report.ok()) {
      return "Preflight passed";
    }
    return "Preflight failed: " + report.issues().stream()
      .map(PreflightReports::formatIssue)
      .collect(Collectors.joining("; "));
  }

  private static String formatIssue(PreflightIssue issue) {
    StringBuilder sb = new StringBuilder();
    sb.append('[').append(issue.code()).append("] ").append(issue.message());
    if (issue.remediation() != null && !issue.remediation().isBlank()) {
      sb.append(" Remediation: ").append(issue.remediation());
    }
    return sb.toString();
  }
}
