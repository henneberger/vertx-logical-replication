package dev.henneberger.vertx.replication.core;

import java.util.Objects;

public final class PreflightIssue {
  public enum Severity {
    ERROR,
    WARNING
  }

  private final Severity severity;
  private final String code;
  private final String message;
  private final String remediation;

  public PreflightIssue(Severity severity, String code, String message, String remediation) {
    this.severity = Objects.requireNonNull(severity, "severity");
    this.code = Objects.requireNonNull(code, "code");
    this.message = Objects.requireNonNull(message, "message");
    this.remediation = remediation;
  }

  public Severity severity() {
    return severity;
  }

  public String code() {
    return code;
  }

  public String message() {
    return message;
  }

  public String remediation() {
    return remediation;
  }
}
