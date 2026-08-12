package io.github.minkin.eventflow.ingestion.domain;

import java.util.UUID;

public class IdempotencyConflictException extends RuntimeException {
    public IdempotencyConflictException(UUID eventId) {
        super("eventId " + eventId + " was previously accepted with different content");
    }
}
