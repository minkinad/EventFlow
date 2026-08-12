package io.github.minkin.eventflow.delivery.job;

import com.fasterxml.jackson.databind.json.JsonMapper;
import io.github.minkin.eventflow.contracts.DeliveryCommand;
import io.github.minkin.eventflow.contracts.DeliveryTarget;
import io.github.minkin.eventflow.contracts.TargetType;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
class DeliveryJobRepositoryTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

    private final JsonMapper objectMapper = JsonMapper.builder().findAndAddModules().build();
    private DeliveryJobRepository repository;

    @BeforeEach
    void setUp() {
        var dataSource = new DriverManagerDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").cleanDisabled(false)
                .load().clean();
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate();
        repository = new DeliveryJobRepository(JdbcClient.create(dataSource), objectMapper);
    }

    @Test
    void duplicateCommandCreatesOneDurableJobAndClaim() throws Exception {
        Instant now = Instant.parse("2026-08-12T10:15:30Z");
        UUID eventId = UUID.randomUUID();
        var command = new DeliveryCommand(1, UUID.randomUUID(), eventId, "order.created", "orders", 1,
                objectMapper.readTree("{\"orderId\":\"42\"}"),
                List.of(new DeliveryTarget(TargetType.POSTGRES, "events", Map.of())), Map.of(), now, null);
        String raw = objectMapper.writeValueAsString(command);

        assertThat(repository.accept(command, raw)).isTrue();
        assertThat(repository.accept(command, raw)).isFalse();

        List<DeliveryJob> jobs = repository.claim(10, "test-worker", 60);
        assertThat(jobs).singleElement().satisfies(job -> {
            assertThat(job.command().eventId()).isEqualTo(eventId);
            assertThat(job.target().type()).isEqualTo(TargetType.POSTGRES);
            assertThat(job.attempt()).isEqualTo(1);
        });
    }
}
