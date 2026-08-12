package io.github.minkin.eventflow.processing.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import io.github.minkin.eventflow.contracts.DeliveryTarget;
import io.github.minkin.eventflow.contracts.TargetType;
import org.springframework.stereotype.Component;

import java.util.ArrayList;

@Component
public class PipelineEngine {
    private final PipelineRepository repository;
    private final EnrichmentGateway enrichmentGateway;
    private final JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);

    public PipelineEngine(PipelineRepository repository, EnrichmentGateway enrichmentGateway) {
        this.repository = repository;
        this.enrichmentGateway = enrichmentGateway;
    }

    public PipelineResult execute(PipelineDefinition pipeline, JsonNode input) {
        JsonNode payload = input.deepCopy();
        var targets = new ArrayList<DeliveryTarget>();
        for (PipelineStepDefinition step : pipeline.steps()) {
            String type = step.type() == null ? "" : step.type().toLowerCase(java.util.Locale.ROOT);
            switch (type) {
                case "validate" -> validate(step, payload);
                case "enrich" -> payload = enrichmentGateway.enrich(required(step.source(), "source"), payload);
                case "route" -> targets.add(route(step));
                default -> throw new PipelineException("UNKNOWN_STEP", "Unknown pipeline step: " + step.type(), false);
            }
        }
        if (targets.isEmpty()) {
            throw new PipelineException("NO_ROUTE", "Pipeline produced no delivery target", false);
        }
        return new PipelineResult(payload, targets);
    }

    private void validate(PipelineStepDefinition step, JsonNode payload) {
        String name = required(step.schema(), "schema");
        JsonNode definition = repository.findSchema(name)
                .orElseThrow(() -> new PipelineException("SCHEMA_NOT_FOUND", "Schema not found: " + name, false));
        var errors = schemaFactory.getSchema(definition).validate(payload);
        if (!errors.isEmpty()) {
            String message = errors.stream().limit(10).map(Object::toString)
                    .collect(java.util.stream.Collectors.joining("; "));
            throw new PipelineException("SCHEMA_VALIDATION_FAILED", message, false);
        }
    }

    private DeliveryTarget route(PipelineStepDefinition step) {
        try {
            return new DeliveryTarget(TargetType.valueOf(required(step.target(), "target").toUpperCase(java.util.Locale.ROOT)),
                    required(step.destination(), "destination"), step.options());
        } catch (IllegalArgumentException exception) {
            throw new PipelineException("INVALID_ROUTE", exception.getMessage(), false, exception);
        }
    }

    private String required(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new PipelineException("INVALID_PIPELINE", "Step field is required: " + field, false);
        }
        return value;
    }
}
