package io.github.minkin.eventflow.contracts;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.json.JsonMapper;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ContractValidationTest {
  private final JsonMapper mapper = JsonMapper.builder().findAndAddModules().build();

  @Test
  void contractsRoundTripWithoutPersistenceTypes() throws Exception {
    var event =
        new EventEnvelope(
            1,
            UUID.randomUUID(),
            "order.created",
            "test",
            1,
            Instant.now(),
            Instant.now(),
            mapper.readTree("{}"),
            Map.of(),
            null);
    ContractValidation.validate(event);
    assertThat(mapper.readValue(mapper.writeValueAsString(event), EventEnvelope.class))
        .isEqualTo(event);
    var command = command(1, List.of(new DeliveryTarget(TargetType.POSTGRES, "events", null)));
    ContractValidation.validate(command);
    assertThat(mapper.readValue(mapper.writeValueAsString(command), DeliveryCommand.class))
        .isEqualTo(command);
  }

  @Test
  void rejectsMissingIdentityUnsupportedVersionAndDuplicateRoutes() {
    assertThatThrownBy(() -> ContractValidation.validate((EventEnvelope) null))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                ContractValidation.validate(
                    new EventEnvelope(
                        1,
                        null,
                        "x",
                        "x",
                        1,
                        Instant.now(),
                        Instant.now(),
                        mapper.createObjectNode(),
                        null,
                        null)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> ContractValidation.validate(command(2, List.of())))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> ContractValidation.validate(command(1, List.of())))
        .isInstanceOf(IllegalArgumentException.class);
    var route = new DeliveryTarget(TargetType.POSTGRES, "events", Map.of());
    assertThatThrownBy(() -> ContractValidation.validate(command(1, List.of(route, route))))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private DeliveryCommand command(int version, List<DeliveryTarget> targets) {
    return new DeliveryCommand(
        version,
        UUID.randomUUID(),
        UUID.randomUUID(),
        "order.created",
        "orders",
        1,
        mapper.createObjectNode(),
        targets,
        null,
        Instant.now(),
        null);
  }
}
