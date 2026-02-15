package dev.henneberger.vertx.replication.core;

public final class OptionValidation {

  private OptionValidation() {
  }

  public static void require(String fieldName, String value) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(fieldName + " is required");
    }
  }

  public static void requirePort(int port) {
    if (port < 1 || port > 65535) {
      throw new IllegalArgumentException("port must be between 1 and 65535");
    }
  }

  public static void requireMin(String fieldName, long value, long minInclusive) {
    if (value < minInclusive) {
      throw new IllegalArgumentException(fieldName + " must be >= " + minInclusive);
    }
  }

  public static void requireMin(String fieldName, int value, int minInclusive) {
    if (value < minInclusive) {
      throw new IllegalArgumentException(fieldName + " must be >= " + minInclusive);
    }
  }
}
