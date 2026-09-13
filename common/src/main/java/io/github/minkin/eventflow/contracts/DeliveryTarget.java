package io.github.minkin.eventflow.contracts;

import java.util.Map;

public record DeliveryTarget(TargetType type, String destination, Map<String, String> options) {
  public DeliveryTarget {
    options = options == null ? Map.of() : Map.copyOf(options);
  }
}
