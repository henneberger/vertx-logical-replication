package dev.henneberger.vertx.neo4j.replication;

import dev.henneberger.vertx.replication.core.AbstractWorkerReplicationStream;
import dev.henneberger.vertx.replication.core.AdapterMode;
import dev.henneberger.vertx.replication.core.ChangeConsumer;
import dev.henneberger.vertx.replication.core.ChangeFilter;
import dev.henneberger.vertx.replication.core.LsnStore;
import dev.henneberger.vertx.replication.core.PreflightIssue;
import dev.henneberger.vertx.replication.core.PreflightReport;
import dev.henneberger.vertx.replication.core.RetryPolicy;
import dev.henneberger.vertx.replication.core.ReplicationStreamState;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.Neo4jException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jLogicalReplicationStream extends AbstractWorkerReplicationStream<Neo4jChangeEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(Neo4jLogicalReplicationStream.class);

  private final Neo4jReplicationOptions options;
  private volatile Driver driver;

  public Neo4jLogicalReplicationStream(Vertx vertx, Neo4jReplicationOptions options) {
    super(vertx);
    this.options = new Neo4jReplicationOptions(Objects.requireNonNull(options, "options"));
    this.options.validate();
  }

  @Override
  public Neo4jChangeSubscription subscribe(ChangeFilter<Neo4jChangeEvent> filter,
                                           ChangeConsumer<Neo4jChangeEvent> eventConsumer,
                                           Handler<Throwable> errorHandler) {
    return () -> registerSubscription(filter, eventConsumer, errorHandler, true).cancel();
  }

  public Neo4jChangeSubscription subscribe(Neo4jChangeFilter filter,
                                           Neo4jChangeConsumer eventConsumer,
                                           Handler<Throwable> errorHandler) {
    return () -> registerSubscription(filter, eventConsumer, errorHandler, true).cancel();
  }

  @Override
  public AdapterMode adapterMode() {
    return AdapterMode.DB_NATIVE_CDC;
  }

  @Override
  protected String streamName() {
    return "neo4j-cdc-" + options.getDatabase();
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
    try (Driver localDriver = GraphDatabase.driver(options.getUri(), AuthTokens.basic(options.getUser(), resolvePassword()));
         Session session = localDriver.session(SessionConfig.builder().withDatabase(options.getDatabase()).build())) {
      session.run("RETURN 1").consume();
    } catch (Neo4jException e) {
      issues.add(new PreflightIssue(
        PreflightIssue.Severity.ERROR,
        "CONNECTION_FAILED",
        "Could not connect to Neo4j: " + e.getMessage(),
        "Verify uri, database, credentials, and access rights."
      ));
    }
    return new PreflightReport(issues);
  }

  @Override
  protected void runSession(long attempt) throws Exception {
    try (Driver localDriver = GraphDatabase.driver(options.getUri(), AuthTokens.basic(options.getUser(), resolvePassword()))) {
      driver = localDriver;
      transition(ReplicationStreamState.RUNNING, null, attempt);
      completeStart();

      String checkpointKey = checkpointKey();
      long lastPosition = parsePosition(loadCheckpoint(checkpointKey));

      while (shouldRun()) {
        List<Neo4jChangeEvent> events = new ArrayList<>();
        try (Session session = localDriver.session(SessionConfig.builder().withDatabase(options.getDatabase()).build())) {
          Map<String, Object> params = new LinkedHashMap<>();
          params.put("lastPosition", lastPosition);
          params.put("limit", options.getBatchSize());
          params.put("source", options.getSourceName());
          Result result = session.run(options.getEventQuery(), params);
          while (result.hasNext()) {
            events.add(toEvent(result.next()));
          }
        }

        if (events.isEmpty()) {
          sleepInterruptibly(options.getPollIntervalMs());
          continue;
        }

        for (Neo4jChangeEvent event : events) {
          dispatchAndAwait(event);
          emitEventMetric(event);
          long current = parsePosition(event.getPosition());
          if (current > lastPosition) {
            lastPosition = current;
            String token = Long.toString(lastPosition);
            saveCheckpoint(checkpointKey, token);
            emitLsnCommitted(checkpointKey, token);
          }
        }
      }
    } finally {
      driver = null;
    }
  }

  @Override
  protected void logStreamFailure(Throwable error) {
    LOG.error("Neo4j CDC stream failed for {}", options.getDatabase(), error);
  }

  @Override
  protected void onCloseResources() {
    Driver current = driver;
    driver = null;
    if (current != null) {
      current.close();
    }
  }

  private Neo4jChangeEvent toEvent(Record record) {
    String source = readString(record, "source", options.getSourceName());
    String rawOperation = readString(record, "operation", "UPDATE");
    Neo4jChangeEvent.Operation operation = mapOperation(rawOperation);
    Map<String, Object> before = readMap(record.get("before"));
    Map<String, Object> after = readMap(record.get("after"));
    String position = readString(record, "position", "");
    Instant commitTs = readInstant(record.get("commitTimestamp"));

    Map<String, Object> metadata = new LinkedHashMap<>();
    metadata.put("adapter", "neo4j");
    metadata.put("rawOperation", rawOperation);
    metadata.put("labels", readObject(record, "labels"));
    metadata.put("entityType", readObject(record, "entityType"));

    return new Neo4jChangeEvent(source, operation, before, after, position, commitTs, metadata);
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

  private String checkpointKey() {
    return "neo4j:" + options.getDatabase() + ":" + options.getSourceName();
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

  private static String readString(Record record, String key, String fallback) {
    if (!record.containsKey(key) || record.get(key).isNull()) {
      return fallback;
    }
    return record.get(key).asString(fallback);
  }

  private static Object readObject(Record record, String key) {
    if (!record.containsKey(key) || record.get(key).isNull()) {
      return null;
    }
    return record.get(key).asObject();
  }

  private static Map<String, Object> readMap(Value value) {
    if (value == null || value.isNull()) {
      return Collections.emptyMap();
    }
    Object raw = value.asObject();
    if (raw instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String, Object> map = (Map<String, Object>) raw;
      return map;
    }
    return Collections.emptyMap();
  }

  private static Instant readInstant(Value value) {
    if (value == null || value.isNull()) {
      return null;
    }
    Object raw = value.asObject();
    if (raw instanceof Instant) {
      return (Instant) raw;
    }
    if (raw instanceof String) {
      try {
        return Instant.parse((String) raw);
      } catch (Exception ignore) {
        return null;
      }
    }
    return null;
  }

  private static Neo4jChangeEvent.Operation mapOperation(String raw) {
    String normalized = raw == null ? "" : raw.toUpperCase(Locale.ROOT);
    if (normalized.startsWith("INS")) {
      return Neo4jChangeEvent.Operation.INSERT;
    }
    if (normalized.startsWith("DEL")) {
      return Neo4jChangeEvent.Operation.DELETE;
    }
    return Neo4jChangeEvent.Operation.UPDATE;
  }
}
