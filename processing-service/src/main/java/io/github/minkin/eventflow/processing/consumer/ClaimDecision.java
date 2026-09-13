package io.github.minkin.eventflow.processing.consumer;

public enum ClaimDecision {
  CLAIMED,
  DUPLICATE,
  BUSY,
  CONTENT_CONFLICT
}
