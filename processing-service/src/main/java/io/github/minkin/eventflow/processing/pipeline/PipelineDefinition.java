package io.github.minkin.eventflow.processing.pipeline;

import java.util.List;

public record PipelineDefinition(
    String name, int version, String eventType, List<PipelineStepDefinition> steps) {
  public PipelineDefinition {
    steps = steps == null ? List.of() : List.copyOf(steps);
  }
}
