package io.github.minkin.eventflow.processing.pipeline;

import com.fasterxml.jackson.databind.JsonNode;

public interface EnrichmentGateway {
    JsonNode enrich(String source, JsonNode payload);
}
