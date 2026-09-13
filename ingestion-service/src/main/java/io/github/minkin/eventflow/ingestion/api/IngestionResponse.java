package io.github.minkin.eventflow.ingestion.api;

import java.time.Instant;
import java.util.UUID;

public record IngestionResponse(
    UUID eventId, String status, boolean duplicate, Instant acceptedAt) {}
