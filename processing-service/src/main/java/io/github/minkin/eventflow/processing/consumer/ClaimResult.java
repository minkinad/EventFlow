package io.github.minkin.eventflow.processing.consumer;

public record ClaimResult(ClaimDecision decision, int attempt) {
}
