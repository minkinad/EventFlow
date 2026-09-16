package io.github.minkin.eventflow.processing.pipeline;

import java.time.Instant;
import java.util.UUID;

public record PipelineSummary(
    UUID id,
    String name,
    int version,
    String eventType,
    boolean active,
    String state,
    long revision,
    Instant createdAt) {}
