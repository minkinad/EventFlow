package io.github.minkin.eventflow.processing.consumer;

import java.util.UUID;

public record ClaimResult(ClaimDecision decision, int attempt, UUID leaseToken) {}
