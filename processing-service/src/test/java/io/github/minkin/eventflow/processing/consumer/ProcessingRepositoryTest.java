package io.github.minkin.eventflow.processing.consumer;

import com.fasterxml.jackson.databind.json.JsonMapper;
import io.github.minkin.eventflow.contracts.DeliveryCommand;
import io.github.minkin.eventflow.contracts.DeliveryTarget;
import io.github.minkin.eventflow.contracts.EventEnvelope;
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
class ProcessingRepositoryTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

    private final JsonMapper objectMapper = JsonMapper.builder().findAndAddModules().build();
    private ProcessingRepository repository;

    @BeforeEach
    void setUp() {
        var dataSource = new DriverManagerDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").cleanDisabled(false)
                .load().clean();
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate();
        repository = new ProcessingRepository(JdbcClient.create(dataSource), objectMapper);
    }

    @Test
    void completedEventIsDeduplicatedWithItsOutboxWrite() throws Exception {
        UUID eventId = UUID.randomUUID();
        Instant now = Instant.parse("2026-08-12T10:15:30Z");
        var envelope = new EventEnvelope(1, eventId, "order.created", "test", 1, now, now,
                objectMapper.readTree("{\"orderId\":\"42\"}"), Map.of(), null);
        String raw = objectMapper.writeValueAsString(envelope);

        assertThat(repository.claim(eventId, "a".repeat(64), raw, 60).decision())
                .isEqualTo(ClaimDecision.CLAIMED);

        var command = new DeliveryCommand(1, UUID.randomUUID(), eventId, "order.created", "orders", 1,
                envelope.payload(), List.of(new DeliveryTarget(TargetType.POSTGRES, "events", Map.of())),
                Map.of(), now, null);
        repository.complete(eventId, "orders", 1, command);

        assertThat(repository.claim(eventId, "a".repeat(64), raw, 60).decision())
                .isEqualTo(ClaimDecision.DUPLICATE);
    }

    @Test
    void retryableAttemptIsReclaimedAndAttemptNumberIncreases() throws Exception {
        UUID eventId = UUID.randomUUID();
        Instant now = Instant.parse("2026-08-12T10:15:30Z");
        var envelope = new EventEnvelope(1, eventId, "order.created", "test", 1, now, now,
                objectMapper.readTree("{\"orderId\":\"42\"}"), Map.of(), null);
        String raw = objectMapper.writeValueAsString(envelope);

        ClaimResult first = repository.claim(eventId, "b".repeat(64), raw, 60);
        repository.releaseForRetry(eventId, "DEPENDENCY_UNAVAILABLE", "temporary failure");
        ClaimResult second = repository.claim(eventId, "b".repeat(64), raw, 60);

        assertThat(first).isEqualTo(new ClaimResult(ClaimDecision.CLAIMED, 1));
        assertThat(second).isEqualTo(new ClaimResult(ClaimDecision.CLAIMED, 2));
    }
}
