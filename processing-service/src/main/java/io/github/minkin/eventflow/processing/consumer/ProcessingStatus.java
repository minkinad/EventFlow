package io.github.minkin.eventflow.processing.consumer;

import java.time.Instant;
import java.util.UUID;

public record ProcessingStatus(
        UUID eventId,
        String status,
        int attempt,
        String pipeline,
        Integer pipelineVersion,
        String failureCode,
        String failureMessage,
        Instant firstSeenAt,
        Instant updatedAt
) {
}
