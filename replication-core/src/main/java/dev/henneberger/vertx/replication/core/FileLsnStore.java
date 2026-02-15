package dev.henneberger.vertx.replication.core;

import io.vertx.core.json.JsonObject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Persists stream checkpoints in a JSON file.
 */
public final class FileLsnStore implements LsnStore {

  private final Path file;
  private final Object monitor = new Object();

  public FileLsnStore(Path file) {
    this.file = Objects.requireNonNull(file, "file");
  }

  @Override
  public Optional<String> load(String streamName) throws Exception {
    Objects.requireNonNull(streamName, "streamName");
    synchronized (monitor) {
      return Optional.ofNullable(readAll().get(streamName));
    }
  }

  @Override
  public void save(String streamName, String lsn) throws Exception {
    Objects.requireNonNull(streamName, "streamName");
    Objects.requireNonNull(lsn, "lsn");

    synchronized (monitor) {
      Map<String, String> values = readAll();
      values.put(streamName, lsn);
      writeAll(values);
    }
  }

  private Map<String, String> readAll() throws IOException {
    if (Files.notExists(file)) {
      return new LinkedHashMap<>();
    }

    String raw = Files.readString(file, StandardCharsets.UTF_8);
    if (raw.isBlank()) {
      return new LinkedHashMap<>();
    }

    JsonObject json = new JsonObject(raw);
    Map<String, String> values = new LinkedHashMap<>();
    for (String key : json.fieldNames()) {
      Object value = json.getValue(key);
      if (value != null) {
        values.put(key, String.valueOf(value));
      }
    }
    return values;
  }

  private void writeAll(Map<String, String> values) throws IOException {
    Path parent = file.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }

    JsonObject json = new JsonObject();
    values.forEach(json::put);
    Files.writeString(file, json.encodePrettily(), StandardCharsets.UTF_8);
  }
}
