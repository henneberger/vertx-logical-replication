package dev.henneberger.vertx.replication.core;

public enum ReplicationStreamState {
  CREATED,
  STARTING,
  RUNNING,
  RETRYING,
  FAILED,
  CLOSED
}
