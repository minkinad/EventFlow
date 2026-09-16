package io.github.minkin.eventflow.processing.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.json.JsonMapper;
import io.github.minkin.eventflow.contracts.TargetType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class PipelineEngineTest {
  private final PipelineRepository repository = mock(PipelineRepository.class);
  private final EnrichmentGateway enrichment = mock(EnrichmentGateway.class);
  private final PipelineEngine engine = new PipelineEngine(repository, enrichment);
  private final JsonMapper mapper = JsonMapper.builder().build();

  private PipelineStepDefinition route() {
    return new PipelineStepDefinition("route", null, null, "POSTGRES", "events", Map.of());
  }

  private PipelineDefinition pipeline(PipelineStepDefinition... steps) {
    return new PipelineDefinition("orders", 1, "order.created", List.of(steps));
  }

  @Test
  void validationPrecedesRoutingWithoutChangingInput() throws Exception {
    when(repository.findSchema("order-v1"))
        .thenReturn(Optional.of(mapper.readTree("{\"type\":\"object\",\"required\":[\"id\"]}")));
    var input = mapper.readTree("{\"id\":\"42\"}");
    var result =
        engine.execute(
            pipeline(
                new PipelineStepDefinition("validate", "order-v1", null, null, null, null),
                route()),
            input);
    assertThat(result.payload()).isEqualTo(input).isNotSameAs(input);
    assertThat(result.targets())
        .singleElement()
        .satisfies(target -> assertThat(target.type()).isEqualTo(TargetType.POSTGRES));
    verifyNoInteractions(enrichment);
  }

  @Test
  void invalidPayloadNeverEnrichesOrRoutes() throws Exception {
    when(repository.findSchema("order-v1"))
        .thenReturn(Optional.of(mapper.readTree("{\"type\":\"object\",\"required\":[\"id\"]}")));
    assertThatThrownBy(
            () ->
                engine.execute(
                    pipeline(
                        new PipelineStepDefinition("validate", "order-v1", null, null, null, null),
                        route()),
                    mapper.createObjectNode()))
        .isInstanceOf(PipelineException.class)
        .hasMessageContaining("id");
    verifyNoInteractions(enrichment);
  }

  @Test
  void unknownSchemaStepTargetAndEmptyRouteAreTerminal() {
    assertThatThrownBy(
            () ->
                engine.execute(
                    pipeline(
                        new PipelineStepDefinition("validate", "missing", null, null, null, null),
                        route()),
                    mapper.createObjectNode()))
        .isInstanceOf(PipelineException.class);
    assertThatThrownBy(
            () ->
                engine.execute(
                    pipeline(new PipelineStepDefinition("script", null, null, null, null, null)),
                    mapper.createObjectNode()))
        .isInstanceOf(PipelineException.class);
    assertThatThrownBy(
            () ->
                engine.execute(
                    pipeline(
                        new PipelineStepDefinition("route", null, null, "UNKNOWN", "events", null)),
                    mapper.createObjectNode()))
        .isInstanceOf(PipelineException.class);
    assertThatThrownBy(() -> engine.execute(pipeline(), mapper.createObjectNode()))
        .isInstanceOf(PipelineException.class)
        .hasMessageContaining("no delivery");
  }

  @Test
  void enrichmentFailurePreservesRetryTaxonomy() {
    when(enrichment.enrich("customers", mapper.createObjectNode()))
        .thenThrow(new PipelineException("TIMEOUT", "timeout", true));
    assertThatThrownBy(
            () ->
                engine.execute(
                    pipeline(
                        new PipelineStepDefinition("enrich", null, "customers", null, null, null),
                        route()),
                    mapper.createObjectNode()))
        .isInstanceOfSatisfying(
            PipelineException.class, failure -> assertThat(failure.retryable()).isTrue());
  }
}
