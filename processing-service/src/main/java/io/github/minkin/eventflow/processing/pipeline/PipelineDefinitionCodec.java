package io.github.minkin.eventflow.processing.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import java.io.IOException;
import org.springframework.stereotype.Component;

@Component
public class PipelineDefinitionCodec {
  private final ObjectMapper mapper;
  private final JsonSchema schema;

  public PipelineDefinitionCodec(ObjectMapper mapper) throws IOException {
    this.mapper = mapper;
    try (var input = getClass().getResourceAsStream("/schemas/pipeline.json")) {
      if (input == null) {
        throw new IllegalStateException("Pipeline schema resource is missing");
      }
      schema =
          JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012)
              .getSchema(mapper.readTree(input));
    }
  }

  public PipelineDefinition decode(JsonNode input) {
    if (input == null || !schema.validate(input).isEmpty()) {
      throw new IllegalArgumentException("Pipeline definition violates its JSON Schema");
    }
    try {
      return mapper.treeToValue(input, PipelineDefinition.class);
    } catch (IOException failure) {
      throw new IllegalArgumentException("Invalid pipeline definition", failure);
    }
  }
}
