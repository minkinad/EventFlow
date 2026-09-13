package io.github.minkin.eventflow.ingestion.api;

import java.time.Instant;
import java.util.UUID;

public record IngestionStatus(
    UUID eventId,
    String eventType,
    String source,
    String status,
    Instant receivedAt,
    Instant publishedAt,
    int publicationAttempts,
    String publicationError) {}
