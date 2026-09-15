package io.github.minkin.eventflow.processing.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import java.util.regex.Pattern;
import org.springframework.stereotype.Component;

@Component
public class SchemaDefinitionValidator {
  private static final Pattern NAME = Pattern.compile("^[a-z][a-z0-9._-]{1,119}$");
  private final JsonSchemaFactory schemaFactory =
      JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);

  public void validate(String name, JsonNode definition) {
    if (!NAME.matcher(name).matches()) {
      throw new IllegalArgumentException("Schema name must match " + NAME.pattern());
    }
    if (definition == null || !definition.isObject()) {
      throw new IllegalArgumentException("JSON Schema must be an object");
    }
    rejectRemoteReferences(definition);
    try {
      schemaFactory.getSchema(definition);
    } catch (RuntimeException exception) {
      throw new IllegalArgumentException(
          "Invalid JSON Schema: " + exception.getMessage(), exception);
    }
  }

  private void rejectRemoteReferences(JsonNode node) {
    if (node.isObject()) {
      if (node.has("$schema")
          && !"https://json-schema.org/draft/2020-12/schema"
              .equals(node.path("$schema").asText())) {
        throw new IllegalArgumentException("Only JSON Schema 2020-12 is supported");
      }
      if (node.has("$id")) {
        throw new IllegalArgumentException("Schema base URI overrides are not supported");
      }

      for (String keyword : java.util.List.of("$ref", "$dynamicRef", "$recursiveRef")) {
        if (node.has(keyword)
            && (!node.get(keyword).isTextual() || !node.get(keyword).asText().startsWith("#"))) {
          throw new IllegalArgumentException("Only document-local schema references are supported");
        }
      }
    }
    if (node.isContainerNode()) {
      node.forEach(this::rejectRemoteReferences);
    }
  }
}
