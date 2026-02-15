package dev.henneberger.vertx.mongodb.replication;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
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
import java.util.Map;
import java.util.Objects;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDbLogicalReplicationStream extends AbstractWorkerReplicationStream<MongoDbChangeEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDbLogicalReplicationStream.class);

  private final MongoDbReplicationOptions options;
  private volatile MongoClient mongoClient;

  public MongoDbLogicalReplicationStream(Vertx vertx, MongoDbReplicationOptions options) {
    super(vertx);
    this.options = new MongoDbReplicationOptions(Objects.requireNonNull(options, "options"));
    this.options.validate();
  }

  @Override
  public MongoDbChangeSubscription subscribe(ChangeFilter<MongoDbChangeEvent> filter,
                                             ChangeConsumer<MongoDbChangeEvent> eventConsumer,
                                             Handler<Throwable> errorHandler) {
    return () -> registerSubscription(filter, eventConsumer, errorHandler, true).cancel();
  }

  public MongoDbChangeSubscription subscribe(MongoDbChangeFilter filter,
                                             MongoDbChangeConsumer eventConsumer,
                                             Handler<Throwable> errorHandler) {
    return () -> registerSubscription(filter, eventConsumer, errorHandler, true).cancel();
  }

  @Override
  public AdapterMode adapterMode() {
    return AdapterMode.LOG_STREAM;
  }

  @Override
  protected String streamName() {
    return "mongodb-cdc-" + options.getCollection();
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
    try (MongoClient client = MongoClients.create(options.getConnectionString())) {
      client.getDatabase(options.getDatabase()).runCommand(new Document("ping", 1));
      client.getDatabase(options.getDatabase()).getCollection(options.getCollection()).estimatedDocumentCount();
    } catch (Exception e) {
      issues.add(new PreflightIssue(
        PreflightIssue.Severity.ERROR,
        "CONNECTION_FAILED",
        "Could not connect to MongoDB change stream source: " + e.getMessage(),
        "Verify connectionString, database, collection, and privileges."
      ));
    }
    return new PreflightReport(issues);
  }

  @Override
  protected void runSession(long attempt) throws Exception {
    try (MongoClient client = MongoClients.create(options.getConnectionString())) {
      mongoClient = client;
      MongoDatabase database = client.getDatabase(options.getDatabase());
      MongoCollection<Document> collection = database.getCollection(options.getCollection());
      ChangeStreamIterable<Document> iterable = collection.watch().batchSize(options.getBatchSize());
      if (options.isFullDocumentLookup()) {
        iterable.fullDocument(FullDocument.UPDATE_LOOKUP);
      }

      String checkpoint = loadCheckpoint(checkpointKey());
      if (!checkpoint.isBlank()) {
        iterable.resumeAfter(BsonDocument.parse(checkpoint));
      }

      transition(ReplicationStreamState.RUNNING, null, attempt);
      completeStart();

      try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = iterable.cursor()) {
        while (shouldRun() && cursor.hasNext()) {
          ChangeStreamDocument<Document> change = cursor.next();
          MongoDbChangeEvent event = toEvent(change);
          dispatchAndAwait(event);
          emitEventMetric(event);

          String token = change.getResumeToken() == null ? "" : change.getResumeToken().toJson();
          if (!token.isBlank()) {
            saveCheckpoint(checkpointKey(), token);
            emitLsnCommitted(checkpointKey(), token);
          }
        }
      }
    } finally {
      mongoClient = null;
    }
  }

  @Override
  protected void logStreamFailure(Throwable error) {
    LOG.error("MongoDB CDC stream failed for {}.{}", options.getDatabase(), options.getCollection(), error);
  }

  @Override
  protected void onCloseResources() {
    MongoClient current = mongoClient;
    mongoClient = null;
    if (current != null) {
      current.close();
    }
  }

  private MongoDbChangeEvent toEvent(ChangeStreamDocument<Document> change) {
    String rawOperation = change.getOperationType() == null ? "" : change.getOperationType().getValue();
    MongoDbChangeEvent.Operation operation = mapOperation(rawOperation);
    Map<String, Object> before = change.getFullDocumentBeforeChange() == null
      ? Collections.emptyMap()
      : change.getFullDocumentBeforeChange();
    Map<String, Object> after = change.getFullDocument() == null
      ? Collections.emptyMap()
      : change.getFullDocument();
    String position = change.getResumeToken() == null ? "" : change.getResumeToken().toJson();
    Instant commitTs = toInstant(change.getClusterTime());

    Map<String, Object> metadata = new LinkedHashMap<>();
    metadata.put("adapter", "mongodb");
    metadata.put("rawOperation", rawOperation);
    if (change.getDocumentKey() != null) {
      metadata.put("documentKey", change.getDocumentKey());
    }
    if (change.getNamespace() != null) {
      metadata.put("namespace", change.getNamespace().getFullName());
    }

    return new MongoDbChangeEvent(
      options.getDatabase() + "." + options.getCollection(),
      operation,
      before,
      after,
      position,
      commitTs,
      metadata
    );
  }

  private String checkpointKey() {
    return "mongodb:" + options.getDatabase() + ":" + options.getCollection();
  }

  private static MongoDbChangeEvent.Operation mapOperation(String raw) {
    if ("insert".equalsIgnoreCase(raw)) {
      return MongoDbChangeEvent.Operation.INSERT;
    }
    if ("delete".equalsIgnoreCase(raw)) {
      return MongoDbChangeEvent.Operation.DELETE;
    }
    return MongoDbChangeEvent.Operation.UPDATE;
  }

  private static Instant toInstant(BsonTimestamp ts) {
    return ts == null ? null : Instant.ofEpochSecond(ts.getTime());
  }
}
