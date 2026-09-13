package io.github.minkin.eventflow.processing.pipeline;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import io.github.minkin.eventflow.contracts.TargetType;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PipelineDefinitionValidatorTest {
  private final PipelineDefinitionValidator validator =
      new PipelineDefinitionValidator(mock(PipelineRepository.class));

  @Test
  void requiresAtLeastOneRoute() {
    var pipeline =
        new PipelineDefinition(
            "orders",
            1,
            "order.created",
            List.of(new PipelineStepDefinition("enrich", null, "customers", null, null, Map.of())));

    assertThatThrownBy(() -> validator.validate(pipeline))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("route");
  }

  @Test
  void rejectsInsecureExternalDestination() {
    var pipeline =
        new PipelineDefinition(
            "orders",
            1,
            "order.created",
            List.of(
                new PipelineStepDefinition(
                    "route",
                    null,
                    null,
                    TargetType.EXTERNAL_HTTP.name(),
                    "http://internal.example/events",
                    Map.of())));

    assertThatThrownBy(() -> validator.validate(pipeline))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("HTTPS");
  }
}
