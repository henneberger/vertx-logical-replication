package dev.henneberger.vertx.cockroachdb.replication;

import dev.henneberger.vertx.replication.core.AbstractWorkerReplicationStream;
import dev.henneberger.vertx.replication.core.AdapterMode;
import dev.henneberger.vertx.replication.core.ChangeConsumer;
import dev.henneberger.vertx.replication.core.ChangeFilter;
import dev.henneberger.vertx.replication.core.LsnStore;
import dev.henneberger.vertx.replication.core.PreflightIssue;
import dev.henneberger.vertx.replication.core.PreflightReport;
import dev.henneberger.vertx.replication.core.ReplicationStateChange;
import dev.henneberger.vertx.replication.core.RetryPolicy;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CockroachDbLogicalReplicationStream extends AbstractWorkerReplicationStream<CockroachDbChangeEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(CockroachDbLogicalReplicationStream.class);

  private final CockroachDbReplicationOptions options;

  private volatile Connection connection;
  private volatile Process changefeedProcess;
  private volatile BufferedReader changefeedReader;

  public CockroachDbLogicalReplicationStream(Vertx vertx, CockroachDbReplicationOptions options) {
    super(vertx);
    this.options = new CockroachDbReplicationOptions(Objects.requireNonNull(options, "options"));
    this.options.validate();
  }

  public CockroachDbChangeSubscription subscribe(CockroachDbChangeFilter filter,
                                                 CockroachDbChangeConsumer eventConsumer,
                                                 Handler<Throwable> errorHandler) {
    return () -> registerSubscription(filter, eventConsumer, errorHandler, true).cancel();
  }

  @Override
  public CockroachDbChangeSubscription subscribe(ChangeFilter<CockroachDbChangeEvent> filter,
                                                 ChangeConsumer<CockroachDbChangeEvent> eventConsumer,
                                                 Handler<Throwable> errorHandler) {
    return () -> registerSubscription(filter, eventConsumer, errorHandler, true).cancel();
  }

  @Override
  public AdapterMode adapterMode() {
    return AdapterMode.LOG_STREAM;
  }

  @Override
  protected String streamName() {
    return "cockroachdb-cdc-" + options.getSourceTable();
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
    try (Connection conn = openConnection()) {
      try (PreparedStatement ps = conn.prepareStatement("SELECT 1"); ResultSet rs = ps.executeQuery()) {
        rs.next();
      }
      checkSourceTableExists(conn, issues);
    } catch (Exception e) {
      issues.add(new PreflightIssue(
        PreflightIssue.Severity.ERROR,
        "CONNECTION_FAILED",
        "Could not connect to CockroachDB: " + e.getMessage(),
        "Verify host, port, database, user, password, and SSL settings."
      ));
    }
    return new PreflightReport(issues);
  }

  @Override
  protected void runSession(long attempt) throws Exception {
    String checkpointKey = checkpointKey();
    String storedCursor = loadCheckpoint(checkpointKey);
    String effectiveCursor = (storedCursor == null || storedCursor.isBlank()) ? options.getInitialCursor() : storedCursor;
    runCliSession(attempt, checkpointKey, effectiveCursor);
  }

  @Override
  protected void logStreamFailure(Throwable error) {
    LOG.error("CockroachDB CDC stream failed for {}", options.getSourceTable(), error);
  }

  @Override
  protected void onCloseResources() {
    BufferedReader reader = changefeedReader;
    changefeedReader = null;
    if (reader != null) {
      try {
        reader.close();
      } catch (Exception ignore) {
      }
    }

    Process process = changefeedProcess;
    changefeedProcess = null;
    if (process != null) {
      try {
        process.destroy();
      } catch (Exception ignore) {
      }
    }

    Connection conn = connection;
    connection = null;
    if (conn != null) {
      try {
        conn.close();
      } catch (Exception ignore) {
      }
    }
  }

  private Connection openConnection() throws Exception {
    String url = "jdbc:postgresql://" + options.getHost() + ":" + options.getPort() + "/" + options.getDatabase();
    return DriverManager.getConnection(url, options.getUser(), resolvePassword());
  }

  private void runCliSession(long attempt, String checkpointKey, String cursor) throws Exception {
    String query = buildChangefeedQuery(cursor);
    List<String> command = buildCliCommand(query);
    Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
    this.changefeedProcess = process;
    this.changefeedReader = reader;
    boolean started = false;

    String line;
    while (shouldRun() && (line = reader.readLine()) != null) {
      String trimmed = line.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      if ("table,key,value".equalsIgnoreCase(trimmed)) {
        if (!started) {
          transition(dev.henneberger.vertx.replication.core.ReplicationStreamState.RUNNING, null, attempt);
          completeStart();
          started = true;
        }
        continue;
      }
      if (trimmed.startsWith("ERROR:")) {
        throw new IllegalStateException("Cockroach changefeed command failed: " + trimmed);
      }

      Map<String, Object> row = parseCsvRow(trimmed);
      if (row == null) {
        continue;
      }
      if (!started) {
        transition(dev.henneberger.vertx.replication.core.ReplicationStreamState.RUNNING, null, attempt);
        completeStart();
        started = true;
      }

      CockroachDbChangeEvent event = mapChangefeedRow(row);
      if (event == null) {
        continue;
      }
      dispatchAndAwait(event);
      emitEventMetric(event);

      String token = event.getPosition();
      if (token != null && !token.isBlank()) {
        saveCheckpoint(checkpointKey, token);
        emitLsnCommitted(checkpointKey, token);
      }
    }

    if (!started && shouldRun()) {
      throw new IllegalStateException("Cockroach changefeed command ended before stream startup");
    }

    if (shouldRun()) {
      int exit = process.waitFor();
      if (exit != 0) {
        throw new IllegalStateException("Cockroach changefeed command exited with code " + exit);
      }
    }
  }

  private List<String> buildCliCommand(String query) {
    List<String> configured = options.getCliCommand();
    List<String> command = new ArrayList<>();
    if (configured.isEmpty()) {
      command.add("cockroach");
      command.add("sql");
      command.add("--insecure");
      command.add("--host=" + options.getHost() + ":" + options.getPort());
      command.add("--database=" + options.getDatabase());
      command.add("--format=csv");
      command.add("-e");
      command.add(query);
      return command;
    }

    boolean replaced = false;
    for (String token : configured) {
      if ("{query}".equals(token)) {
        command.add(query);
        replaced = true;
      } else {
        command.add(token);
      }
    }
    if (!replaced) {
      command.add(query);
    }
    return command;
  }

  private String buildChangefeedQuery(String cursor) {
    Map<String, Object> opts = new LinkedHashMap<>(options.getChangefeedOptions());
    opts.putIfAbsent("envelope", "wrapped");
    opts.putIfAbsent("diff", true);
    opts.putIfAbsent("updated", true);
    opts.putIfAbsent("resolved", "1s");
    if (cursor != null && !cursor.isBlank()) {
      opts.putIfAbsent("cursor", cursor);
    }

    StringBuilder query = new StringBuilder("EXPERIMENTAL CHANGEFEED FOR TABLE ")
      .append(options.getSourceTable());

    if (!opts.isEmpty()) {
      query.append(" WITH ");
      boolean first = true;
      for (Map.Entry<String, Object> entry : opts.entrySet()) {
        if (!first) {
          query.append(", ");
        }
        first = false;
        query.append(entry.getKey());
        Object value = entry.getValue();
        if (value instanceof Boolean) {
          if (!((Boolean) value)) {
            query.append(" = false");
          }
          continue;
        }
        if (value instanceof Number) {
          query.append(" = ").append(value);
          continue;
        }
        if (value != null) {
          query.append(" = '").append(escapeSql(value.toString())).append("'");
        }
      }
    }

    return query.toString();
  }

  private CockroachDbChangeEvent mapChangefeedRow(Map<String, Object> row) {
    String source = asString(getAny(row, "table", "topic"));
    if (source == null || source.isBlank()) {
      source = options.getSourceTable();
    }

    Object valueRaw = getAny(row, "value", "v");
    if (valueRaw == null) {
      return null;
    }

    JsonObject envelope = parseEnvelope(valueRaw);
    if (envelope == null) {
      return null;
    }
    if (envelope.containsKey("resolved") && !envelope.containsKey("before") && !envelope.containsKey("after")) {
      return null;
    }

    Map<String, Object> before = asMap(envelope.getValue("before"));
    Map<String, Object> after = asMap(envelope.getValue("after"));
    String updated = asString(getAny(row, "updated", "mvcc_timestamp"));
    if (updated == null || updated.isBlank()) {
      updated = envelope.getString("updated");
    }

    CockroachDbChangeEvent.Operation operation = operationFor(before, after);
    Map<String, Object> metadata = new LinkedHashMap<>();
    metadata.put("adapter", "cockroachdb");
    metadata.put("rawKey", toText(getAny(row, "key", "k")));
    metadata.put("rawValue", toText(valueRaw));

    return new CockroachDbChangeEvent(
      source,
      operation,
      before,
      after,
      updated,
      parseInstant(updated),
      metadata
    );
  }

  private void checkSourceTableExists(Connection conn, List<PreflightIssue> issues) throws Exception {
    String table = options.getSourceTable();
    String schema = "public";
    if (table.contains(".")) {
      int idx = table.indexOf('.');
      schema = table.substring(0, idx);
      table = table.substring(idx + 1);
    }

    try (PreparedStatement ps = conn.prepareStatement(
      "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?")) {
      ps.setString(1, schema);
      ps.setString(2, table);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          issues.add(new PreflightIssue(
            PreflightIssue.Severity.ERROR,
            "SOURCE_TABLE_MISSING",
            "Source table '" + options.getSourceTable() + "' was not found",
            "Create the table and verify sourceTable configuration."
          ));
        }
      }
    }
  }

  private String checkpointKey() {
    return "cockroachdb:" + options.getDatabase() + ":" + options.getSourceTable();
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

  private static String escapeSql(String value) {
    return value.replace("'", "''");
  }

  private static Object getAny(Map<String, Object> row, String... keys) {
    for (String key : keys) {
      if (row.containsKey(key)) {
        return row.get(key);
      }
    }
    return null;
  }

  private static String asString(Object value) {
    if (value == null) {
      return null;
    }
    return String.valueOf(value);
  }

  private static JsonObject parseEnvelope(Object raw) {
    try {
      String text = toText(raw);
      if (text == null || text.isBlank() || "null".equalsIgnoreCase(text.trim())) {
        return null;
      }
      return new JsonObject(text);
    } catch (Exception ignore) {
      return null;
    }
  }

  private static Map<String, Object> asMap(Object raw) {
    if (raw == null) {
      return Map.of();
    }
    if (raw instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String, Object> map = (Map<String, Object>) raw;
      return new LinkedHashMap<>(map);
    }
    if (raw instanceof JsonObject) {
      return ((JsonObject) raw).getMap();
    }
    try {
      return new JsonObject(String.valueOf(raw)).getMap();
    } catch (Exception ignore) {
      return Map.of();
    }
  }

  private static CockroachDbChangeEvent.Operation operationFor(Map<String, Object> before, Map<String, Object> after) {
    boolean hasBefore = before != null && !before.isEmpty();
    boolean hasAfter = after != null && !after.isEmpty();
    if (hasAfter && !hasBefore) {
      return CockroachDbChangeEvent.Operation.INSERT;
    }
    if (!hasAfter && hasBefore) {
      return CockroachDbChangeEvent.Operation.DELETE;
    }
    return CockroachDbChangeEvent.Operation.UPDATE;
  }

  private static Instant parseInstant(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    String trimmed = value.trim();
    int dot = trimmed.indexOf('.');
    String numeric = dot >= 0 ? trimmed.substring(0, dot) : trimmed;
    if (!numeric.isBlank() && numeric.chars().allMatch(Character::isDigit)) {
      try {
        long epochNanos = Long.parseLong(numeric);
        long epochSeconds = epochNanos / 1_000_000_000L;
        long nanosAdjustment = epochNanos % 1_000_000_000L;
        return Instant.ofEpochSecond(epochSeconds, nanosAdjustment);
      } catch (Exception ignore) {
      }
    }
    try {
      return Instant.parse(value);
    } catch (Exception ignore) {
      try {
        return Timestamp.valueOf(value.replace('T', ' ').replace("Z", "")).toInstant();
      } catch (Exception ignoreAgain) {
        return null;
      }
    }
  }

  private static String toText(Object raw) {
    if (raw == null) {
      return null;
    }
    if (raw instanceof byte[]) {
      return new String((byte[]) raw, StandardCharsets.UTF_8);
    }
    if (raw instanceof Array) {
      try {
        return toText(((Array) raw).getArray());
      } catch (Exception ignore) {
        return String.valueOf(raw);
      }
    }
    String text = String.valueOf(raw);
    String decoded = decodeHexText(text);
    return decoded != null ? decoded : text;
  }

  private static String decodeHexText(String text) {
    if (text == null || text.length() < 3 || !text.startsWith("\\x")) {
      return null;
    }
    String hex = text.substring(2);
    if ((hex.length() & 1) != 0) {
      return null;
    }
    byte[] out = new byte[hex.length() / 2];
    for (int i = 0; i < hex.length(); i += 2) {
      int hi = Character.digit(hex.charAt(i), 16);
      int lo = Character.digit(hex.charAt(i + 1), 16);
      if (hi < 0 || lo < 0) {
        return null;
      }
      out[i / 2] = (byte) ((hi << 4) + lo);
    }
    return new String(out, StandardCharsets.UTF_8);
  }

  private static Map<String, Object> parseCsvRow(String line) {
    List<String> fields = splitCsv(line);
    if (fields.size() < 3) {
      return null;
    }
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("table", csvNull(fields.get(0)));
    row.put("key", csvNull(fields.get(1)));
    row.put("value", csvNull(fields.get(2)));
    return row;
  }

  private static String csvNull(String value) {
    return value == null || "NULL".equalsIgnoreCase(value) ? null : value;
  }

  private static List<String> splitCsv(String line) {
    List<String> values = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    boolean inQuotes = false;
    for (int i = 0; i < line.length(); i++) {
      char ch = line.charAt(i);
      if (ch == '"') {
        if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
          current.append('"');
          i++;
        } else {
          inQuotes = !inQuotes;
        }
        continue;
      }
      if (ch == ',' && !inQuotes) {
        values.add(current.toString());
        current.setLength(0);
        continue;
      }
      current.append(ch);
    }
    values.add(current.toString());
    return values;
  }
}
