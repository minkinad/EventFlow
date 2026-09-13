package io.github.minkin.eventflow.processing.pipeline;

import io.github.minkin.eventflow.contracts.TargetType;
import java.net.URI;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;
import org.springframework.stereotype.Component;

@Component
public class PipelineDefinitionValidator {
  private static final Pattern IDENTIFIER = Pattern.compile("^[a-z][a-z0-9._-]{1,119}$");
  private static final int MAX_STEPS = 32;
  private final PipelineRepository repository;

  public PipelineDefinitionValidator(PipelineRepository repository) {
    this.repository = repository;
  }

  public void validate(PipelineDefinition pipeline) {
    requireIdentifier(pipeline.name(), "name");
    requireIdentifier(pipeline.eventType(), "eventType");
    if (pipeline.version() < 1) {
      throw new IllegalArgumentException("Pipeline version must be positive");
    }
    if (pipeline.steps().isEmpty() || pipeline.steps().size() > MAX_STEPS) {
      throw new IllegalArgumentException(
          "Pipeline must contain between 1 and " + MAX_STEPS + " steps");
    }

    boolean routeSeen = false;
    Set<String> routes = new HashSet<>();
    for (int index = 0; index < pipeline.steps().size(); index++) {
      PipelineStepDefinition step = pipeline.steps().get(index);
      String type = required(step.type(), "steps[" + index + "].type").toLowerCase(Locale.ROOT);
      switch (type) {
        case "validate" -> {
          if (routeSeen) {
            throw new IllegalArgumentException("Validation cannot run after a route step");
          }
          String schema = required(step.schema(), "steps[" + index + "].schema");
          if (repository.findSchema(schema).isEmpty()) {
            throw new IllegalArgumentException("Active schema not found: " + schema);
          }
        }
        case "enrich" -> {
          if (routeSeen) {
            throw new IllegalArgumentException("Enrichment cannot run after a route step");
          }
          requireIdentifier(required(step.source(), "steps[" + index + "].source"), "source");
        }
        case "route" -> {
          routeSeen = true;
          TargetType target;
          try {
            target =
                TargetType.valueOf(
                    required(step.target(), "steps[" + index + "].target")
                        .toUpperCase(Locale.ROOT));
          } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("Unsupported target in step " + index, exception);
          }
          String destination = required(step.destination(), "steps[" + index + "].destination");
          if (!routes.add(target + ":" + destination)) {
            throw new IllegalArgumentException("Duplicate route: " + target + ":" + destination);
          }
          if (target == TargetType.EXTERNAL_HTTP) {
            URI uri = URI.create(destination);
            if (!"https".equalsIgnoreCase(uri.getScheme()) || uri.getHost() == null) {
              throw new IllegalArgumentException(
                  "External HTTP destination must be an absolute HTTPS URL");
            }
          } else {
            requireIdentifier(destination, "destination");
          }
        }
        default -> throw new IllegalArgumentException("Unsupported pipeline step: " + type);
      }
    }
    if (!routeSeen) {
      throw new IllegalArgumentException("Pipeline must contain at least one route step");
    }
  }

  private void requireIdentifier(String value, String field) {
    if (value == null || !IDENTIFIER.matcher(value).matches()) {
      throw new IllegalArgumentException(field + " must match " + IDENTIFIER.pattern());
    }
  }

  private String required(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(field + " is required");
    }
    return value;
  }
}
