package dev.henneberger.vertx.replication.core;

@FunctionalInterface
public interface ValueNormalizer {
  Object normalize(String fieldName, Object value);
}
