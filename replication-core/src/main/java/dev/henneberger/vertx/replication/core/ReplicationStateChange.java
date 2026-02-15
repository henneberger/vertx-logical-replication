package dev.henneberger.vertx.replication.core;

public final class ReplicationStateChange {
  private final ReplicationStreamState previousState;
  private final ReplicationStreamState state;
  private final Throwable cause;
  private final long attempt;

  public ReplicationStateChange(ReplicationStreamState previousState,
                                ReplicationStreamState state,
                                Throwable cause,
                                long attempt) {
    this.previousState = previousState;
    this.state = state;
    this.cause = cause;
    this.attempt = attempt;
  }

  public ReplicationStreamState previousState() {
    return previousState;
  }

  public ReplicationStreamState state() {
    return state;
  }

  public Throwable cause() {
    return cause;
  }

  public long attempt() {
    return attempt;
  }
}
