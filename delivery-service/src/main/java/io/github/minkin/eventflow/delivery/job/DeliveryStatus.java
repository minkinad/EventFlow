package io.github.minkin.eventflow.delivery.job;

import java.time.Instant;
import java.util.UUID;

public record DeliveryStatus(
    UUID jobId,
    UUID eventId,
    String targetType,
    String destination,
    String status,
    int attempt,
    Instant nextAttemptAt,
    Instant deliveredAt,
    String lastError) {}
