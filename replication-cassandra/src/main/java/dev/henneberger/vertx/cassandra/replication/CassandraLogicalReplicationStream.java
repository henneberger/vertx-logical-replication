package dev.henneberger.vertx.cassandra.replication;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import dev.henneberger.vertx.replication.core.AbstractWorkerReplicationStream;
import dev.henneberger.vertx.replication.core.AdapterMode;
import dev.henneberger.vertx.replication.core.ChangeConsumer;
import dev.henneberger.vertx.replication.core.ChangeFilter;
import dev.henneberger.vertx.replication.core.LsnStore;
import dev.henneberger.vertx.replication.core.PreflightIssue;
import dev.henneberger.vertx.replication.core.PreflightReport;
import dev.henneberger.vertx.replication.core.RetryPolicy;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraLogicalReplicationStream extends AbstractWorkerReplicationStream<CassandraChangeEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraLogicalReplicationStream.class);

  private final CassandraReplicationOptions options;
  private volatile CqlSession session;

  public CassandraLogicalReplicationStream(Vertx vertx, CassandraReplicationOptions options) {
    super(vertx);
    this.options = new CassandraReplicationOptions(Objects.requireNonNull(options, "options"));
    this.options.validate();
  }

  @Override
  public CassandraChangeSubscription subscribe(ChangeFilter<CassandraChangeEvent> filter,
                                               ChangeConsumer<CassandraChangeEvent> eventConsumer,
                                               Handler<Throwable> errorHandler) {
    return () -> registerSubscription(filter, eventConsumer, errorHandler, true).cancel();
  }

  public CassandraChangeSubscription subscribe(CassandraChangeFilter filter,
                                               CassandraChangeConsumer eventConsumer,
                                               Handler<Throwable> errorHandler) {
    return () -> registerSubscription(filter, eventConsumer, errorHandler, true).cancel();
  }

  @Override
  public AdapterMode adapterMode() {
    return AdapterMode.DB_NATIVE_CDC;
  }

  @Override
  protected String streamName() {
    return "cassandra-cdc-" + options.getSourceTable();
  }

  @Override
  protected int maxConcurrentDispatch() {
    return options.getMaxConcurrentDispatch();
  }

  @Override
  protected boolean preflightEnabled() {
    return options.isPreflightEnabled();
  }

  @Override
  protected boolean autoStart() {
    return options.isAutoStart();
  }

  @Override
  protected RetryPolicy retryPolicy() {
    return options.getRetryPolicy();
  }

  @Override
  protected LsnStore checkpointStore() {
    return options.getLsnStore();
  }

  @Override
  protected PreflightReport runPreflightChecks() {
    List<PreflightIssue> issues = new ArrayList<>();
    try (CqlSession localSession = openSession()) {
      TableMetadata tableMetadata = localSession.getMetadata()
        .getKeyspace(CqlIdentifier.fromCql(options.getKeyspace()))
        .flatMap(k -> k.getTable(CqlIdentifier.fromCql(options.getSourceTable())))
        .orElse(null);
      if (tableMetadata == null) {
        issues.add(new PreflightIssue(
          PreflightIssue.Severity.ERROR,
          "SOURCE_TABLE_MISSING",
          "Source table '" + options.getKeyspace() + "." + options.getSourceTable() + "' was not found",
          "Create the table and verify keyspace/sourceTable configuration."
        ));
      }
    } catch (Exception e) {
      issues.add(new PreflightIssue(
        PreflightIssue.Severity.ERROR,
        "CONNECTION_FAILED",
        "Could not connect to Cassandra: " + e.getMessage(),
        "Verify host, port, datacenter, keyspace, and credentials."
      ));
    }
    return new PreflightReport(issues);
  }

  @Override
  protected void runSession(long attempt) throws Exception {
    try (CqlSession localSession = openSession()) {
      session = localSession;
      String cql = "SELECT * FROM " + options.getKeyspace() + "." + options.getSourceTable()
        + " WHERE " + options.getPositionColumn() + " > ? LIMIT ? ALLOW FILTERING";
      PreparedStatement statement = localSession.prepare(cql);

      transition(dev.henneberger.vertx.replication.core.ReplicationStreamState.RUNNING, null, attempt);
      completeStart();

      String checkpointKey = checkpointKey();
      long lastPosition = parsePosition(loadCheckpoint(checkpointKey));
      while (shouldRun()) {
        BoundStatement bound = statement.bind(lastPosition, options.getBatchSize());
        ResultSet rs = localSession.execute(bound);
        List<CassandraChangeEvent> events = new ArrayList<>();
        for (Row row : rs) {
          events.add(mapRow(row));
        }

        if (events.isEmpty()) {
          sleepInterruptibly(options.getPollIntervalMs());
          continue;
        }

        for (CassandraChangeEvent event : events) {
          dispatchAndAwait(event);
          emitEventMetric(event);
          long pos = parsePosition(event.getPosition());
          if (pos > lastPosition) {
            lastPosition = pos;
            String token = Long.toString(lastPosition);
            saveCheckpoint(checkpointKey, token);
            emitLsnCommitted(checkpointKey, token);
          }
        }
      }
    } finally {
      session = null;
    }
  }

  @Override
  protected void logStreamFailure(Throwable error) {
    LOG.error("Cassandra CDC stream failed for {}.{}", options.getKeyspace(), options.getSourceTable(), error);
  }

  @Override
  protected void onCloseResources() {
    CqlSession current = session;
    session = null;
    if (current != null) {
      current.close();
    }
  }

  private CqlSession openSession() {
    CqlSessionBuilder builder = CqlSession.builder()
      .addContactPoint(new InetSocketAddress(options.getHost(), options.getPort()))
      .withLocalDatacenter(options.getLocalDatacenter())
      .withKeyspace(options.getKeyspace());
    if (options.getUser() != null && !options.getUser().isBlank()) {
      builder = builder.withAuthCredentials(options.getUser(), resolvePassword());
    }
    return builder.build();
  }

  private CassandraChangeEvent mapRow(Row row) {
    Map<String, Object> after = new LinkedHashMap<>();
    row.getColumnDefinitions().forEach(def -> after.put(def.getName().asInternal(), row.getObject(def.getName())));
    String rawOperation = after.getOrDefault(options.getOperationColumn(), "UPDATE").toString();
    CassandraChangeEvent.Operation op = mapOperation(rawOperation);
    String position = String.valueOf(after.getOrDefault(options.getPositionColumn(), "0"));
    Map<String, Object> metadata = new LinkedHashMap<>();
    metadata.put("adapter", "cassandra");
    metadata.put("rawOperation", rawOperation);
    return new CassandraChangeEvent(
      options.getKeyspace() + "." + options.getSourceTable(),
      op,
      Map.of(),
      after,
      position,
      Instant.now(),
      metadata
    );
  }

  private String checkpointKey() {
    return "cassandra:" + options.getKeyspace() + ":" + options.getSourceTable();
  }

  private String resolvePassword() {
    String password = options.getPassword();
    if (password == null || password.isBlank()) {
      String env = options.getPasswordEnv();
      if (env != null && !env.isBlank()) {
        password = System.getenv(env);
      }
    }
    return password == null ? "" : password;
  }

  private static long parsePosition(String token) {
    if (token == null || token.isBlank()) {
      return 0L;
    }
    try {
      return Long.parseLong(token);
    } catch (Exception ignore) {
      return 0L;
    }
  }

  private static CassandraChangeEvent.Operation mapOperation(String raw) {
    String normalized = raw == null ? "" : raw.toUpperCase(Locale.ROOT);
    if (normalized.startsWith("INS")) {
      return CassandraChangeEvent.Operation.INSERT;
    }
    if (normalized.startsWith("DEL")) {
      return CassandraChangeEvent.Operation.DELETE;
    }
    return CassandraChangeEvent.Operation.UPDATE;
  }
}
