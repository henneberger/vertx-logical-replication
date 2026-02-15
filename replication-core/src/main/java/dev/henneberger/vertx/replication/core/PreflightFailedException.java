package dev.henneberger.vertx.replication.core;

import java.util.Objects;

public final class PreflightFailedException extends IllegalStateException {

  private final PreflightReport report;
  private final ReplicationStreamState state;

  public PreflightFailedException(PreflightReport report, ReplicationStreamState state) {
    super(PreflightReports.describeFailure(Objects.requireNonNull(report, "report")));
    this.report = report;
    this.state = state == null ? ReplicationStreamState.STARTING : state;
  }

  public PreflightReport report() {
    return report;
  }

  public ReplicationStreamState state() {
    return state;
  }
}
