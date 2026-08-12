package io.github.minkin.eventflow.ingestion.outbox;

import java.util.UUID;

public record OutboxMessage(UUID id, String topic, String messageKey, String payload) {
}
