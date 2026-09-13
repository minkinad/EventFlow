package io.github.minkin.eventflow.ingestion.api;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PastOrPresent;
import jakarta.validation.constraints.Size;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record EventSubmission(
    @NotNull UUID eventId,
    @NotBlank @Size(max = 120) String eventType,
    @NotBlank @Size(max = 120) String source,
    @Min(1) @Max(10_000) int schemaVersion,
    @NotNull @PastOrPresent Instant occurredAt,
    @NotNull JsonNode payload,
    Map<@Size(max = 80) String, @Size(max = 500) String> metadata) {
  public EventSubmission {
    metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
  }
}
