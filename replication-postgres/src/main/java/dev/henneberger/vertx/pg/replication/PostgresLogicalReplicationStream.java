/*
 * Copyright (C) 2026 Daniel Henneberger
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.henneberger.vertx.pg.replication;

import dev.henneberger.vertx.replication.core.ChangeConsumer;
import dev.henneberger.vertx.replication.core.ChangeFilter;
import dev.henneberger.vertx.replication.core.PreflightIssue;
import dev.henneberger.vertx.replication.core.PreflightReport;
import dev.henneberger.vertx.replication.core.PreflightReports;
import dev.henneberger.vertx.replication.core.ReplicationStateChange;
import dev.henneberger.vertx.replication.core.ReplicationStream;
import dev.henneberger.vertx.replication.core.ReplicationStreamState;
import dev.henneberger.vertx.replication.core.ReplicationSubscription;
import dev.henneberger.vertx.replication.core.RetryPolicy;
import dev.henneberger.vertx.replication.core.SubscriptionRegistration;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logical replication stream backed by PostgreSQL.
 */
public class PostgresLogicalReplicationStream implements ReplicationStream<PostgresChangeEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(PostgresLogicalReplicationStream.class);
  private static final long SLOT_LAG_WARNING_BYTES = 128L * 1024L * 1024L;

  private final Vertx vertx;
  private final PostgresReplicationOptions options;
  private final List<ListenerRegistration> listeners = new CopyOnWriteArrayList<>();
  private final List<Handler<ReplicationStateChange>> stateHandlers = new CopyOnWriteArrayList<>();
  private final List<ReplicationMetricsListener> metricsListeners = new CopyOnWriteArrayList<>();
  private final AtomicBoolean shouldRun = new AtomicBoolean(false);

  private volatile java.sql.Connection replConnection;
  private volatile PGReplicationStream replicationStream;
  private volatile Thread worker;
  private volatile Promise<Void> startPromise;
  private volatile ReplicationStreamState state = ReplicationStreamState.CREATED;

  public PostgresLogicalReplicationStream(Vertx vertx, PostgresReplicationOptions options) {
    this.vertx = Objects.requireNonNull(vertx, "vertx");
    this.options = new PostgresReplicationOptions(Objects.requireNonNull(options, "options"));
    this.options.validate();
  }

  @Override
  public Future<Void> start() {
    Promise<Void> promiseToReturn;
    synchronized (this) {
      if (state == ReplicationStreamState.CLOSED) {
        return Future.failedFuture("stream is closed");
      }
      if (state == ReplicationStreamState.RUNNING) {
        return Future.succeededFuture();
      }
      if ((state == ReplicationStreamState.STARTING || state == ReplicationStreamState.RETRYING)
        && startPromise != null) {
        return startPromise.future();
      }

      shouldRun.set(true);
      startPromise = Promise.promise();
      promiseToReturn = startPromise;
      transition(ReplicationStreamState.STARTING, null, 0);
    }

    Future<Void> preflightFuture;
    if (options.isPreflightEnabled()) {
      preflightFuture = preflight().compose(report -> {
        if (report.ok()) {
          return Future.succeededFuture();
        }
        return Future.failedFuture(new dev.henneberger.vertx.replication.core.PreflightFailedException(report, ReplicationStreamState.STARTING));
      });
    } else {
      preflightFuture = Future.succeededFuture();
    }

    preflightFuture.onSuccess(v -> startWorker())
      .onFailure(err -> {
        transition(ReplicationStreamState.FAILED, err, 0);
        shouldRun.set(false);
        failStart(err);
      });

    return promiseToReturn.future();
  }

  @Override
  public Future<PreflightReport> preflight() {
    return vertx.executeBlocking(() -> runPreflightChecks());
  }

  @Override
  public ReplicationStreamState state() {
    return state;
  }

  @Override
  public PostgresChangeSubscription onStateChange(Handler<ReplicationStateChange> handler) {
    Handler<ReplicationStateChange> resolved = Objects.requireNonNull(handler, "handler");
    stateHandlers.add(resolved);
    return () -> stateHandlers.remove(resolved);
  }

  public PostgresChangeSubscription addMetricsListener(ReplicationMetricsListener listener) {
    ReplicationMetricsListener resolved = Objects.requireNonNull(listener, "listener");
    metricsListeners.add(resolved);
    return () -> metricsListeners.remove(resolved);
  }

  public void setMetricsListener(ReplicationMetricsListener listener) {
    metricsListeners.clear();
    if (listener != null) {
      metricsListeners.add(listener);
    }
  }

  @Override
  public ReplicationSubscription addMetricsListener(dev.henneberger.vertx.replication.core.ReplicationMetricsListener<PostgresChangeEvent> listener) {
    if (listener instanceof ReplicationMetricsListener) {
      return addMetricsListener((ReplicationMetricsListener) listener);
    }
    ReplicationMetricsListener adapter = new ReplicationMetricsListener() {
      @Override
      public void onEvent(PostgresChangeEvent event) {
        listener.onEvent(event);
      }

      @Override
      public void onParseFailure(String payload, Throwable error) {
        listener.onParseFailure(payload, error);
      }

      @Override
      public void onStateChange(ReplicationStateChange stateChange) {
        listener.onStateChange(stateChange);
      }

      @Override
      public void onLsnCommitted(String slotName, String lsn) {
        listener.onLsnCommitted(slotName, lsn);
      }
    };
    return addMetricsListener(adapter);
  }

  public PostgresChangeSubscription subscribe(PostgresChangeFilter filter,
                                              PostgresChangeConsumer eventConsumer,
                                              Handler<Throwable> errorHandler) {
    return registerSubscription(filter, eventConsumer, errorHandler, true);
  }

  @Override
  public ReplicationSubscription subscribe(ChangeFilter<PostgresChangeEvent> filter,
                                           ChangeConsumer<PostgresChangeEvent> eventConsumer,
                                           Handler<Throwable> errorHandler) {
    return registerSubscription(filter::test, eventConsumer::handle, errorHandler, true);
  }

  public SubscriptionRegistration startAndSubscribe(PostgresChangeFilter filter,
                                                    PostgresChangeConsumer eventConsumer,
                                                    Handler<Throwable> errorHandler) {
    PostgresChangeSubscription subscription = registerSubscription(filter, eventConsumer, errorHandler, false);
    Future<Void> started = start().onFailure(err -> {
      subscription.cancel();
      if (errorHandler != null) {
        errorHandler.handle(err);
      }
    });
    return new SubscriptionRegistration(subscription, started);
  }

  public SubscriptionRegistration startAndSubscribe(PostgresChangeFilter filter,
                                                    Handler<PostgresChangeEvent> eventHandler,
                                                    Handler<Throwable> errorHandler) {
    Objects.requireNonNull(eventHandler, "eventHandler");
    return startAndSubscribe(filter, event -> {
      eventHandler.handle(event);
      return Future.succeededFuture();
    }, errorHandler);
  }

  @Override
  public SubscriptionRegistration startAndSubscribe(ChangeFilter<PostgresChangeEvent> filter,
                                                    ChangeConsumer<PostgresChangeEvent> eventConsumer,
                                                    Handler<Throwable> errorHandler) {
    ReplicationSubscription subscription = registerSubscription(filter::test, eventConsumer::handle, errorHandler, false);
    Future<Void> started = start().onFailure(err -> {
      subscription.cancel();
      if (errorHandler != null) {
        errorHandler.handle(err);
      }
    });
    return new SubscriptionRegistration(subscription, started);
  }

  private PostgresChangeSubscription registerSubscription(ChangeFilter<PostgresChangeEvent> filter,
                                                          ChangeConsumer<PostgresChangeEvent> eventConsumer,
                                                          Handler<Throwable> errorHandler,
                                                          boolean withAutoStart) {
    Objects.requireNonNull(filter, "filter");
    Objects.requireNonNull(eventConsumer, "eventConsumer");

    ListenerRegistration registration = new ListenerRegistration(filter, eventConsumer, errorHandler);
    listeners.add(registration);

    if (withAutoStart && options.isAutoStart()) {
      start().onFailure(err -> {
        if (errorHandler != null) {
          errorHandler.handle(err);
        }
      });
    }

    return () -> listeners.remove(registration);
  }

  public PostgresChangeSubscription subscribe(PostgresChangeFilter filter,
                                              Handler<PostgresChangeEvent> eventHandler,
                                              Handler<Throwable> errorHandler) {
    return subscribe(filter, event -> {
      eventHandler.handle(event);
      return Future.succeededFuture();
    }, errorHandler);
  }

  @Override
  public dev.henneberger.vertx.replication.core.AdapterMode adapterMode() {
    return dev.henneberger.vertx.replication.core.AdapterMode.LOG_STREAM;
  }

  @Override
  public synchronized void close() {
    shouldRun.set(false);
    transition(ReplicationStreamState.CLOSED, null, 0);

    closeStream();

    Thread thread = worker;
    worker = null;
    if (thread != null) {
      thread.interrupt();
    }

    Promise<Void> currentStartPromise = startPromise;
    startPromise = null;
    if (currentStartPromise != null && !currentStartPromise.future().isComplete()) {
      currentStartPromise.fail("stream closed before reaching RUNNING");
    }
  }

  private synchronized void startWorker() {
    if (!shouldRun.get()) {
      return;
    }
    if (worker != null && worker.isAlive()) {
      return;
    }

    worker = new Thread(this::runLoop, "pg-repl-" + options.getSlotName());
    worker.setDaemon(true);
    worker.start();
  }

  private void runLoop() {
    long attempt = 0;

    try {
      while (shouldRun.get()) {
        attempt++;
        transition(ReplicationStreamState.STARTING, null, attempt);
        try {
          runSession(attempt);
          if (!shouldRun.get()) {
            return;
          }
          throw new IllegalStateException("replication session ended unexpectedly");
        } catch (Exception e) {
          if (!shouldRun.get() || isExpectedShutdown(e)) {
            return;
          }

          notifyError(e);
          LOG.error("PostgreSQL replication stream failed for slot {}", options.getSlotName(), e);

          RetryPolicy retryPolicy = options.getRetryPolicy();
          if (!retryPolicy.shouldRetry(e, attempt)) {
            transition(ReplicationStreamState.FAILED, e, attempt);
            failStart(e);
            shouldRun.set(false);
            return;
          }

          transition(ReplicationStreamState.RETRYING, e, attempt);
          sleepInterruptibly(retryPolicy.computeDelayMillis(attempt));
        }
      }
    } finally {
      closeStream();
      synchronized (this) {
        worker = null;
      }
    }
  }

  private void runSession(long attempt) throws Exception {
    String slotName = options.getSlotName();

    try (java.sql.Connection replConn = openReplicationConnection()) {
      this.replConnection = replConn;

      ensureReplicationSlot();
      PGConnection pgConnection = replConn.unwrap(PGConnection.class);
      LogSequenceNumber lsn = resolveStartLsn();

      PGReplicationStream stream = openReplicationStream(pgConnection, slotName, lsn);
      this.replicationStream = stream;

      transition(ReplicationStreamState.RUNNING, null, attempt);
      completeStart();

      while (shouldRun.get()) {
        ByteBuffer buffer = stream.readPending();
        if (buffer == null) {
          emitStatus(stream);
          sleepInterruptibly(50);
          continue;
        }

        String message = decodeWalMessage(buffer);
        if (message == null || message.isBlank()) {
          emitStatus(stream);
          continue;
        }

        String receiveLsn = stream.getLastReceiveLSN() == null ? null : stream.getLastReceiveLSN().asString();
        List<PostgresChangeEvent> events;
        try {
          events = options.getChangeDecoder().decode(message, receiveLsn);
        } catch (RuntimeException parseError) {
          emitParseFailure(message, parseError);
          throw parseError;
        }

        for (PostgresChangeEvent event : events) {
          dispatchAndAwait(event);
          emitEventMetric(event);
        }

        commitLsn(stream);
      }
    }
  }

  private PGReplicationStream openReplicationStream(PGConnection pgConnection,
                                                    String slotName,
                                                    LogSequenceNumber lsn) throws SQLException {
    ChainedLogicalStreamBuilder builder = pgConnection.getReplicationAPI()
      .replicationStream()
      .logical()
      .withSlotName(slotName)
      .withStartPosition(lsn);

    if (options.getPluginOptions().isEmpty() && "wal2json".equalsIgnoreCase(options.getPlugin())) {
      builder.withSlotOption("include-xids", false)
        .withSlotOption("include-timestamp", true)
        .withSlotOption("include-types", false)
        .withSlotOption("include-typmod", false)
        .withSlotOption("format-version", 2);
    } else {
      for (Map.Entry<String, Object> entry : options.getPluginOptions().entrySet()) {
        applySlotOption(builder, entry.getKey(), entry.getValue());
      }
    }

    return builder.start();
  }

  private void applySlotOption(ChainedLogicalStreamBuilder builder, String key, Object value) {
    if (value instanceof Boolean) {
      builder.withSlotOption(key, (Boolean) value);
      return;
    }
    if (value instanceof Number) {
      builder.withSlotOption(key, ((Number) value).intValue());
      return;
    }
    builder.withSlotOption(key, String.valueOf(value));
  }

  private void dispatchAndAwait(PostgresChangeEvent event) throws Exception {
    List<ListenerRegistration> matching = new ArrayList<>();
    for (ListenerRegistration listener : listeners) {
      if (listener.filter.test(event)) {
        matching.add(listener);
      }
    }
    if (matching.isEmpty()) {
      return;
    }

    int chunkSize = Math.max(1, options.getMaxConcurrentDispatch());
    for (int start = 0; start < matching.size(); start += chunkSize) {
      int end = Math.min(matching.size(), start + chunkSize);
      CountDownLatch latch = new CountDownLatch(end - start);
      AtomicReference<Throwable> failure = new AtomicReference<>();

      for (int idx = start; idx < end; idx++) {
        ListenerRegistration listener = matching.get(idx);
        vertx.runOnContext(v -> invokeListener(event, listener, latch, failure));
      }

      latch.await();
      Throwable err = failure.get();
      if (err != null) {
        if (err instanceof Exception) {
          throw (Exception) err;
        }
        throw new RuntimeException(err);
      }
    }
  }

  private void invokeListener(PostgresChangeEvent event,
                              ListenerRegistration listener,
                              CountDownLatch latch,
                              AtomicReference<Throwable> failure) {
    try {
      Future<Void> result = listener.eventConsumer.handle(event);
      if (result == null) {
        result = Future.succeededFuture();
      }
      result.onComplete(ar -> {
        if (ar.failed()) {
          Throwable err = ar.cause();
          notifyListenerError(listener, err);
          failure.compareAndSet(null, err);
        }
        latch.countDown();
      });
    } catch (Throwable err) {
      notifyListenerError(listener, err);
      failure.compareAndSet(null, err);
      latch.countDown();
    }
  }

  private void notifyListenerError(ListenerRegistration listener, Throwable error) {
    if (listener.errorHandler != null) {
      try {
        listener.errorHandler.handle(error);
      } catch (Throwable ignored) {
        // ignore secondary listener failure
      }
    }
  }

  private void notifyError(Throwable error) {
    for (ListenerRegistration listener : listeners) {
      if (listener.errorHandler != null) {
        vertx.runOnContext(v -> listener.errorHandler.handle(error));
      }
    }
  }

  private void closeStream() {
    PGReplicationStream stream = this.replicationStream;
    this.replicationStream = null;
    if (stream != null) {
      try {
        stream.close();
      } catch (SQLException ignore) {
        // Ignore on close.
      }
    }

    java.sql.Connection replConn = this.replConnection;
    this.replConnection = null;
    if (replConn != null) {
      try {
        replConn.close();
      } catch (SQLException ignore) {
        // Ignore on close.
      }
    }
  }

  private void ensureReplicationSlot() throws SQLException {
    try (java.sql.Connection conn = openStandardConnection();
         PreparedStatement statement = conn.prepareStatement(
           "SELECT pg_create_logical_replication_slot(?, ?)")
    ) {
      statement.setString(1, options.getSlotName());
      statement.setString(2, options.getPlugin());
      try {
        statement.execute();
      } catch (SQLException createError) {
        if (isSlotAlreadyExists(createError)) {
          return;
        }
        throw createError;
      }
    }
  }

  private LogSequenceNumber resolveStartLsn() throws Exception {
    Optional<String> fromStore = options.getLsnStore().load(options.getSlotName());
    if (fromStore.isPresent()) {
      try {
        return LogSequenceNumber.valueOf(fromStore.get());
      } catch (RuntimeException ignored) {
        LOG.warn("Stored LSN '{}' is invalid, falling back to current WAL", fromStore.get());
      }
    }
    return fetchCurrentWal();
  }

  private LogSequenceNumber fetchCurrentWal() throws SQLException {
    try (java.sql.Connection conn = openStandardConnection();
         PreparedStatement statement = conn.prepareStatement("SELECT pg_current_wal_lsn()");
         ResultSet rs = statement.executeQuery()) {
      if (!rs.next()) {
        throw new SQLException("Could not read current WAL LSN");
      }
      return LogSequenceNumber.valueOf(rs.getString(1));
    }
  }

  private java.sql.Connection openReplicationConnection() throws SQLException {
    Properties props = connectionProperties();
    PGProperty.REPLICATION.set(props, "database");
    PGProperty.PREFER_QUERY_MODE.set(props, "simple");
    PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
    return DriverManager.getConnection(postgresJdbcUrl(), props);
  }

  private java.sql.Connection openStandardConnection() throws SQLException {
    return DriverManager.getConnection(postgresJdbcUrl(), connectionProperties());
  }

  private Properties connectionProperties() {
    Properties props = new Properties();
    PGProperty.USER.set(props, options.getUser());

    String password = resolvePassword();
    if (password != null && !password.isBlank()) {
      PGProperty.PASSWORD.set(props, password);
    }

    if (Boolean.TRUE.equals(options.getSsl())) {
      props.setProperty("ssl", "true");
    }
    return props;
  }

  private void emitStatus(PGReplicationStream stream) throws SQLException {
    LogSequenceNumber lsn = stream.getLastReceiveLSN();
    if (lsn == null) {
      return;
    }
    stream.setAppliedLSN(lsn);
    stream.setFlushedLSN(lsn);
    stream.forceUpdateStatus();
  }

  private void commitLsn(PGReplicationStream stream) throws Exception {
    LogSequenceNumber lsn = stream.getLastReceiveLSN();
    if (lsn == null) {
      return;
    }

    stream.setAppliedLSN(lsn);
    stream.setFlushedLSN(lsn);
    stream.forceUpdateStatus();

    String lsnString = lsn.asString();
    options.getLsnStore().save(options.getSlotName(), lsnString);
    emitLsnCommitted(lsnString);
  }

  private String postgresJdbcUrl() {
    return "jdbc:postgresql://" + options.getHost() + ':' + options.getPort() + '/' + options.getDatabase();
  }

  private String resolvePassword() {
    String password = options.getPassword();
    if (password == null || password.isBlank()) {
      String envName = options.getPasswordEnv();
      if (envName != null && !envName.isBlank()) {
        password = System.getenv(envName);
      }
    }
    return password;
  }

  private boolean isExpectedShutdown(Exception error) {
    if (shouldRun.get()) {
      return false;
    }

    String message = error.getMessage();
    return message != null && message.contains("replication stream has been closed");
  }

  private static boolean isSlotAlreadyExists(SQLException error) {
    String state = error.getSQLState();
    if ("42710".equals(state)) {
      return true;
    }
    String message = error.getMessage();
    return message != null && message.contains("already exists");
  }

  private static String decodeWalMessage(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private void transition(ReplicationStreamState nextState, Throwable cause, long attempt) {
    ReplicationStreamState previous = this.state;
    if (previous == nextState && cause == null) {
      return;
    }

    this.state = nextState;
    ReplicationStateChange change = new ReplicationStateChange(previous, nextState, cause, attempt);

    emitStateMetric(change);

    for (Handler<ReplicationStateChange> handler : stateHandlers) {
      vertx.runOnContext(v -> handler.handle(change));
    }
  }

  private void failStart(Throwable error) {
    Promise<Void> promise = startPromise;
    if (promise != null && !promise.future().isComplete()) {
      promise.fail(error);
    }
  }

  private void completeStart() {
    Promise<Void> promise = startPromise;
    if (promise != null && !promise.future().isComplete()) {
      promise.complete();
    }
  }

  private void sleepInterruptibly(long millis) {
    if (millis <= 0) {
      return;
    }
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ignored) {
      Thread.currentThread().interrupt();
    }
  }

  private PreflightReport runPreflightChecks() {
    List<PreflightIssue> issues = new ArrayList<>();

    if (!options.getChangeDecoder().supportsPlugin(options.getPlugin())) {
      issues.add(new PreflightIssue(
        PreflightIssue.Severity.ERROR,
        "DECODER_PLUGIN_MISMATCH",
        "Decoder '" + options.getChangeDecoder().getClass().getSimpleName()
          + "' does not support plugin '" + options.getPlugin() + "'",
        "Use a compatible decoder for the configured plugin."
      ));
    }

    try (java.sql.Connection conn = openStandardConnection()) {
      checkWalLevel(conn, issues);
      checkRolePrivileges(conn, issues);
      checkReplicationSettings(conn, issues);
      checkExistingSlot(conn, issues);
      checkSlotLag(conn, issues);
      checkPluginExtensionVisibility(conn, issues);
    } catch (Exception e) {
      issues.add(new PreflightIssue(
        PreflightIssue.Severity.ERROR,
        "CONNECTION_FAILED",
        "Could not connect to PostgreSQL: " + e.getMessage(),
        "Verify host, port, database, user, password, and SSL settings."
      ));
    }

    return new PreflightReport(issues);
  }

  private void checkWalLevel(java.sql.Connection conn, List<PreflightIssue> issues) throws SQLException {
    try (PreparedStatement statement = conn.prepareStatement("SHOW wal_level");
         ResultSet rs = statement.executeQuery()) {
      if (!rs.next()) {
        issues.add(new PreflightIssue(
          PreflightIssue.Severity.ERROR,
          "WAL_LEVEL_UNKNOWN",
          "Could not read wal_level",
          "Set wal_level=logical and restart PostgreSQL."
        ));
        return;
      }

      String walLevel = rs.getString(1);
      if (!"logical".equalsIgnoreCase(walLevel)) {
        issues.add(new PreflightIssue(
          PreflightIssue.Severity.ERROR,
          "WAL_LEVEL_INVALID",
          "wal_level is '" + walLevel + "'",
          "Set wal_level=logical and restart PostgreSQL."
        ));
      }
    }
  }

  private void checkRolePrivileges(java.sql.Connection conn, List<PreflightIssue> issues) throws SQLException {
    try (PreparedStatement statement = conn.prepareStatement(
      "SELECT (rolreplication OR rolsuper) FROM pg_roles WHERE rolname = current_user");
         ResultSet rs = statement.executeQuery()) {
      if (rs.next() && !rs.getBoolean(1)) {
        issues.add(new PreflightIssue(
          PreflightIssue.Severity.WARNING,
          "ROLE_NOT_REPLICATION",
          "Current user does not have replication privileges",
          "Grant REPLICATION privilege or use a superuser role."
        ));
      }
    }
  }

  private void checkReplicationSettings(java.sql.Connection conn, List<PreflightIssue> issues) throws SQLException {
    checkPositiveSetting(conn, "max_replication_slots", issues, "MAX_REPLICATION_SLOTS_INVALID");
    checkPositiveSetting(conn, "max_wal_senders", issues, "MAX_WAL_SENDERS_INVALID");
  }

  private void checkPositiveSetting(java.sql.Connection conn,
                                    String setting,
                                    List<PreflightIssue> issues,
                                    String code) throws SQLException {
    try (PreparedStatement statement = conn.prepareStatement("SHOW " + setting);
         ResultSet rs = statement.executeQuery()) {
      if (rs.next()) {
        long value = rs.getLong(1);
        if (value < 1) {
          issues.add(new PreflightIssue(
            PreflightIssue.Severity.ERROR,
            code,
            setting + " is set to " + value,
            "Set " + setting + " to at least 1 and restart PostgreSQL."
          ));
        }
      }
    }
  }

  private void checkExistingSlot(java.sql.Connection conn, List<PreflightIssue> issues) throws SQLException {
    try (PreparedStatement statement = conn.prepareStatement(
      "SELECT plugin FROM pg_replication_slots WHERE slot_name = ?")) {
      statement.setString(1, options.getSlotName());
      try (ResultSet rs = statement.executeQuery()) {
        if (rs.next()) {
          String slotPlugin = rs.getString(1);
          if (!options.getPlugin().equalsIgnoreCase(slotPlugin)) {
            issues.add(new PreflightIssue(
              PreflightIssue.Severity.ERROR,
              "SLOT_PLUGIN_MISMATCH",
              "Replication slot uses plugin '" + slotPlugin + "' but configured plugin is '" + options.getPlugin() + "'",
              "Use a slot created with the configured plugin, or configure the matching plugin name."
            ));
          }
        }
      }
    }
  }

  private void checkSlotLag(java.sql.Connection conn, List<PreflightIssue> issues) throws SQLException {
    try (PreparedStatement statement = conn.prepareStatement(
      "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) "
        + "FROM pg_replication_slots WHERE slot_name = ? AND restart_lsn IS NOT NULL")) {
      statement.setString(1, options.getSlotName());
      try (ResultSet rs = statement.executeQuery()) {
        if (rs.next()) {
          long lagBytes = rs.getLong(1);
          if (lagBytes > SLOT_LAG_WARNING_BYTES) {
            issues.add(new PreflightIssue(
              PreflightIssue.Severity.WARNING,
              "SLOT_LAG_HIGH",
              "Replication slot lag is " + lagBytes + " bytes",
              "Ensure consumers are keeping up or recreate the slot if appropriate."
            ));
          }
        }
      }
    }
  }

  private void checkPluginExtensionVisibility(java.sql.Connection conn, List<PreflightIssue> issues) throws SQLException {
    try (PreparedStatement statement = conn.prepareStatement(
      "SELECT 1 FROM pg_available_extensions WHERE name = ?")) {
      statement.setString(1, options.getPlugin());
      try (ResultSet rs = statement.executeQuery()) {
        if (!rs.next()) {
          issues.add(new PreflightIssue(
            PreflightIssue.Severity.WARNING,
            "PLUGIN_NOT_VISIBLE_AS_EXTENSION",
            "Plugin '" + options.getPlugin() + "' is not listed in pg_available_extensions",
            "This may be normal for output plugins; verify plugin availability on the server if startup fails."
          ));
        }
      }
    }
  }

  private void emitEventMetric(PostgresChangeEvent event) {
    for (ReplicationMetricsListener listener : metricsListeners) {
      listener.onEvent(event);
    }
  }

  private void emitParseFailure(String payload, Throwable error) {
    for (ReplicationMetricsListener listener : metricsListeners) {
      listener.onParseFailure(payload, error);
    }
  }

  private void emitStateMetric(ReplicationStateChange change) {
    for (ReplicationMetricsListener listener : metricsListeners) {
      listener.onStateChange(change);
    }
  }

  private void emitLsnCommitted(String lsn) {
    for (ReplicationMetricsListener listener : metricsListeners) {
      listener.onLsnCommitted(options.getSlotName(), lsn);
    }
  }

  private static final class ListenerRegistration {

    private final ChangeFilter<PostgresChangeEvent> filter;
    private final ChangeConsumer<PostgresChangeEvent> eventConsumer;
    private final Handler<Throwable> errorHandler;

    private ListenerRegistration(ChangeFilter<PostgresChangeEvent> filter,
                                 ChangeConsumer<PostgresChangeEvent> eventConsumer,
                                 Handler<Throwable> errorHandler) {
      this.filter = filter;
      this.eventConsumer = eventConsumer;
      this.errorHandler = errorHandler;
    }
  }
}
