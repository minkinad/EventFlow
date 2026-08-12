package io.github.minkin.eventflow.processing.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.minkin.eventflow.contracts.DeliveryTarget;

import java.util.List;

public record PipelineResult(JsonNode payload, List<DeliveryTarget> targets) {
}
