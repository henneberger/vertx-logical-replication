package dev.henneberger.vertx.replication.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class PreflightReport {
  private final List<PreflightIssue> issues;

  public PreflightReport(List<PreflightIssue> issues) {
    Objects.requireNonNull(issues, "issues");
    this.issues = Collections.unmodifiableList(new ArrayList<>(issues));
  }

  public boolean ok() {
    for (PreflightIssue issue : issues) {
      if (issue.severity() == PreflightIssue.Severity.ERROR) {
        return false;
      }
    }
    return true;
  }

  public List<PreflightIssue> issues() {
    return issues;
  }
}
