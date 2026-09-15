package io.github.minkin.eventflow.processing.pipeline;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.json.JsonMapper;
import org.junit.jupiter.api.Test;

class SchemaDefinitionValidatorTest {
  @Test
  void cannotFetchSchemaFromMetadataOrExternalNetwork() throws Exception {
    var validator = new SchemaDefinitionValidator();
    var mapper = JsonMapper.builder().build();
    for (String reference :
        new String[] {
          "http://169.254.169.254/latest", "https://example.com/schema", "file:///etc/passwd"
        }) {
      var schema = mapper.createObjectNode();
      schema.putObject("properties").putObject("secret").put("$ref", reference);
      assertThatThrownBy(() -> validator.validate("test-v1", schema))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("document-local");
    }
    validator.validate(
        "test-v1",
        mapper.readTree(
            "{\"type\":\"object\",\"$defs\":{\"name\":{\"type\":\"string\"}},\"properties\":{\"name\":{\"$ref\":\"#/$defs/name\"}}}"));
  }
}
