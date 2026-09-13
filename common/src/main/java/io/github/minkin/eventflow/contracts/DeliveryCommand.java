package io.github.minkin.eventflow.contracts;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public record DeliveryCommand(
    int contractVersion,
    UUID commandId,
    UUID eventId,
    String eventType,
    String pipeline,
    int pipelineVersion,
    JsonNode payload,
    List<DeliveryTarget> targets,
    Map<String, String> metadata,
    Instant processedAt,
    String traceparent) {
  public DeliveryCommand {
    targets = targets == null ? List.of() : List.copyOf(targets);
    metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
  }
}
