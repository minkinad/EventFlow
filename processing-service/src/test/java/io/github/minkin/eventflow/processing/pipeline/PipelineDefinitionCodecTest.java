package io.github.minkin.eventflow.processing.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.json.JsonMapper;
import org.junit.jupiter.api.Test;

class PipelineDefinitionCodecTest {
  @Test
  void rejectsUnknownFieldsScriptsAndWrongStepParameters() throws Exception {
    var mapper = JsonMapper.builder().build();
    var codec = new PipelineDefinitionCodec(mapper);
    var json =
        mapper.readTree(
            "{\"name\":\"orders\",\"version\":1,\"eventType\":\"order.created\",\"steps\":[{\"type\":\"route\",\"target\":\"POSTGRES\",\"destination\":\"events\"}]}");
    assertThat(codec.decode(json).steps()).hasSize(1);
    for (String field : new String[] {"javaClass", "script", "command", "unknown"}) {
      var invalid = json.deepCopy();
      ((com.fasterxml.jackson.databind.node.ObjectNode) invalid.path("steps").get(0))
          .put(field, "forbidden");
      assertThatThrownBy(() -> codec.decode(invalid)).isInstanceOf(IllegalArgumentException.class);
    }
    var invalid = json.deepCopy();
    ((com.fasterxml.jackson.databind.node.ObjectNode) invalid.path("steps").get(0))
        .put("schema", "inapplicable-to-route");
    assertThatThrownBy(() -> codec.decode(invalid)).isInstanceOf(IllegalArgumentException.class);
  }
}
