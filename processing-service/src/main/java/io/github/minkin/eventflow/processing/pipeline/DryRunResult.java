package io.github.minkin.eventflow.processing.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.minkin.eventflow.contracts.DeliveryTarget;
import java.util.List;

public record DryRunResult(
    boolean valid,
    List<StepResult> executedSteps,
    JsonNode payload,
    List<DeliveryTarget> selectedRoutes,
    List<String> warnings,
    List<String> errors,
    long durationNanos) {
  public DryRunResult {
    executedSteps = List.copyOf(executedSteps);
    selectedRoutes = List.copyOf(selectedRoutes);
    warnings = List.copyOf(warnings);
    errors = List.copyOf(errors);
  }

  public record StepResult(int index, String type, String status, JsonNode result) {}
}
