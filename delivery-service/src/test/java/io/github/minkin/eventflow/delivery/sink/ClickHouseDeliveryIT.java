package io.github.minkin.eventflow.delivery.sink;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.json.JsonMapper;
import io.github.minkin.eventflow.contracts.DeliveryCommand;
import io.github.minkin.eventflow.contracts.DeliveryTarget;
import io.github.minkin.eventflow.contracts.TargetType;
import io.github.minkin.eventflow.delivery.job.DeliveryJob;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class ClickHouseDeliveryIT {
  @Container
  static final GenericContainer<?> CLICKHOUSE =
      new GenericContainer<>("clickhouse/clickhouse-server:25.3-alpine")
          .withEnv("CLICKHOUSE_USER", "eventflow")
          .withEnv("CLICKHOUSE_PASSWORD", "test-password")
          .withExposedPorts(8123)
          .waitingFor(Wait.forHttp("/ping"));

  @Test
  void retryDeduplicatesWithFinalAndIndependentDestinationsRemainDistinct() throws Exception {
    String ddl = Files.readString(Path.of("..", "infrastructure", "clickhouse", "init.sql"));
    var init =
        CLICKHOUSE.execInContainer(
            "clickhouse-client",
            "--user",
            "eventflow",
            "--password",
            "test-password",
            "--multiquery",
            "--query",
            ddl);
    assertThat(init.getExitCode()).as(init.getStderr()).isZero();
    String url = "http://" + CLICKHOUSE.getHost() + ":" + CLICKHOUSE.getMappedPort(8123);
    var mapper = JsonMapper.builder().findAndAddModules().build();
    var adapter =
        new ClickHouseDeliveryAdapter(
            RestClient.builder(), mapper, url, "eventflow", "test-password");
    var first = new DeliveryTarget(TargetType.CLICKHOUSE, "first", Map.of());
    var second = new DeliveryTarget(TargetType.CLICKHOUSE, "second", Map.of());
    var command =
        new DeliveryCommand(
            1,
            UUID.randomUUID(),
            UUID.randomUUID(),
            "order.created",
            "orders",
            1,
            mapper.createObjectNode(),
            List.of(first, second),
            Map.of(),
            Instant.now(),
            null);
    var job = new DeliveryJob(UUID.randomUUID(), command, first, 1, UUID.randomUUID());
    adapter.deliver(job);
    adapter.deliver(job);
    adapter.deliver(new DeliveryJob(UUID.randomUUID(), command, second, 1, UUID.randomUUID()));
    var result =
        CLICKHOUSE.execInContainer(
            "clickhouse-client",
            "--user",
            "eventflow",
            "--password",
            "test-password",
            "--query",
            "SELECT count() FROM eventflow.events FINAL WHERE event_id='"
                + command.eventId()
                + "'");
    assertThat(result.getExitCode()).isZero();
    assertThat(result.getStdout().trim()).isEqualTo("2");
  }
}
