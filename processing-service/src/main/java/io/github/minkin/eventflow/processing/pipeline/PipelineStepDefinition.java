package io.github.minkin.eventflow.processing.pipeline;

import java.util.Map;

public record PipelineStepDefinition(
        String type,
        String schema,
        String source,
        String target,
        String destination,
        Map<String, String> options
) {
    public PipelineStepDefinition {
        options = options == null ? Map.of() : Map.copyOf(options);
    }
}
