package io.github.minkin.eventflow.processing.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
public class HttpEnrichmentGateway implements EnrichmentGateway {
  private final RestClient restClient;

  public HttpEnrichmentGateway(
      RestClient.Builder builder,
      @Value("${eventflow.enrichment.base-url:http://localhost:8090}") String baseUrl) {
    this.restClient = builder.baseUrl(baseUrl).build();
  }

  @Override
  @Retry(name = "enrichment")
  @CircuitBreaker(name = "enrichment")
  public JsonNode enrich(String source, JsonNode payload) {
    JsonNode key = payload.path("customerId");
    if (key.isMissingNode() || key.asText().isBlank()) {
      throw new PipelineException(
          "ENRICHMENT_KEY_MISSING", "customerId is required for enrichment", false);
    }
    try {
      JsonNode value =
          restClient
              .get()
              .uri("/api/v1/enrichments/{source}/{key}", source, key.asText())
              .retrieve()
              .body(JsonNode.class);
      ObjectNode enriched = payload.deepCopy();
      enriched.set("_enrichment", value);
      return enriched;
    } catch (PipelineException exception) {
      throw exception;
    } catch (RuntimeException exception) {
      throw new PipelineException(
          "ENRICHMENT_UNAVAILABLE", "Enrichment source is unavailable: " + source, true, exception);
    }
  }
}
