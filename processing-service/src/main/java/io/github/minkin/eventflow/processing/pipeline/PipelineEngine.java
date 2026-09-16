package io.github.minkin.eventflow.processing.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import io.github.minkin.eventflow.contracts.DeliveryTarget;
import io.github.minkin.eventflow.contracts.TargetType;
import java.util.ArrayList;
import org.springframework.stereotype.Component;

@Component
public class PipelineEngine {
  private final PipelineRepository repository;
  private final EnrichmentGateway enrichmentGateway;
  private final JsonSchemaFactory schemaFactory =
      JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);

  public PipelineEngine(PipelineRepository repository, EnrichmentGateway enrichmentGateway) {
    this.repository = repository;
    this.enrichmentGateway = enrichmentGateway;
  }

  public PipelineResult execute(PipelineDefinition pipeline, JsonNode input) {
    var result = run(pipeline, input, false);
    return new PipelineResult(result.payload(), result.selectedRoutes());
  }

  public DryRunResult dryRun(PipelineDefinition pipeline, JsonNode input) {
    return run(pipeline, input, true);
  }

  private DryRunResult run(PipelineDefinition pipeline, JsonNode input, boolean dryRun) {
    if (input == null || input.isNull()) {
      throw new IllegalArgumentException("A test payload is required");
    }
    long started = System.nanoTime();
    JsonNode payload = input.deepCopy();
    var targets = new ArrayList<DeliveryTarget>();
    var steps = new ArrayList<DryRunResult.StepResult>();
    var warnings = new ArrayList<String>();
    var errors = new ArrayList<String>();
    for (int index = 0; index < pipeline.steps().size(); index++) {
      var step = pipeline.steps().get(index);
      String type = step.type() == null ? "" : step.type().toLowerCase(java.util.Locale.ROOT);
      String status = "SUCCEEDED";
      try {
        switch (type) {
          case "validate" -> validate(step, payload);
          case "enrich" -> {
            if (dryRun) {
              status = "SKIPPED";
              warnings.add(
                  "External enrichment at step "
                      + index
                      + " was skipped; this dry-run is incomplete");
            } else {
              payload = enrichmentGateway.enrich(required(step.source(), "source"), payload);
            }
          }
          case "route" -> targets.add(route(step));
          default ->
              throw new PipelineException("UNKNOWN_STEP", "Unsupported pipeline step", false);
        }
      } catch (PipelineException failure) {
        if (!dryRun) {
          throw failure;
        }
        errors.add(failure.code() + ": " + failure.getMessage());
        steps.add(new DryRunResult.StepResult(index, type, "FAILED", payload.deepCopy()));
        break;
      }
      if (dryRun) {
        steps.add(new DryRunResult.StepResult(index, type, status, payload.deepCopy()));
      }
    }
    if (targets.isEmpty() && errors.isEmpty()) {
      if (!dryRun) {
        throw new PipelineException("NO_ROUTE", "Pipeline produced no delivery target", false);
      }
      errors.add("NO_ROUTE: Pipeline produced no delivery target");
    }
    return new DryRunResult(
        errors.isEmpty() && warnings.isEmpty(),
        steps,
        payload,
        targets,
        warnings,
        errors,
        System.nanoTime() - started);
  }

  private void validate(PipelineStepDefinition step, JsonNode payload) {
    String name = required(step.schema(), "schema");
    JsonNode definition =
        repository
            .findSchema(name)
            .orElseThrow(
                () ->
                    new PipelineException("SCHEMA_NOT_FOUND", "Schema not found: " + name, false));
    try {
      new SchemaDefinitionValidator().validate(name, definition);
    } catch (IllegalArgumentException failure) {
      throw new PipelineException(
          "INVALID_SCHEMA", "Stored schema violates reference policy", false, failure);
    }
    var errors = schemaFactory.getSchema(definition).validate(payload);
    if (!errors.isEmpty()) {
      String message =
          errors.stream()
              .limit(10)
              .map(Object::toString)
              .collect(java.util.stream.Collectors.joining("; "));
      throw new PipelineException("SCHEMA_VALIDATION_FAILED", message, false);
    }
  }

  private DeliveryTarget route(PipelineStepDefinition step) {
    try {
      return new DeliveryTarget(
          TargetType.valueOf(required(step.target(), "target").toUpperCase(java.util.Locale.ROOT)),
          required(step.destination(), "destination"),
          step.options());
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
