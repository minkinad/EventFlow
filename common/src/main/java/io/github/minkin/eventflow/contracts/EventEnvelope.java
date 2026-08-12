package io.github.minkin.eventflow.contracts;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record EventEnvelope(
        int contractVersion,
        UUID eventId,
        String eventType,
        String source,
        int schemaVersion,
        Instant occurredAt,
        Instant receivedAt,
        JsonNode payload,
        Map<String, String> metadata,
        String traceparent
) {
    public EventEnvelope {
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }
}
