package io.github.minkin.eventflow.contracts;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

public record DeadLetterEvent(
    int contractVersion,
    UUID deadLetterId,
    UUID eventId,
    String stage,
    String failureCode,
    String failureMessage,
    int attempt,
    JsonNode originalMessage,
    Instant failedAt,
    String traceparent) {}
