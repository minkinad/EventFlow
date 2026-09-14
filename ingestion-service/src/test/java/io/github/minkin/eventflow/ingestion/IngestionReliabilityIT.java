package io.github.minkin.eventflow.ingestion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.minkin.eventflow.ingestion.api.EventSubmission;
import io.github.minkin.eventflow.ingestion.domain.CanonicalJson;
import io.github.minkin.eventflow.ingestion.domain.EventIngestionService;
import io.github.minkin.eventflow.ingestion.domain.IdempotencyConflictException;
import io.github.minkin.eventflow.ingestion.outbox.OutboxRepository;
import io.github.minkin.eventflow.ingestion.persistence.IngestionRepository;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IngestionReliabilityIT extends PostgresIntegrationSupport {
  private EventIngestionService service;
  private IngestionRepository repository;
  private OutboxRepository outbox;
  private EventSubmission event;

  @BeforeEach
  void setup() throws Exception {
    repository = new IngestionRepository(jdbc, mapper);
    service =
        transactional(new EventIngestionService(repository, new CanonicalJson(mapper), mapper));
    outbox = transactional(new OutboxRepository(jdbc));
    event =
        new EventSubmission(
            UUID.randomUUID(),
            "order.created",
            "test",
            1,
            Instant.now(),
            mapper.readTree("{\"a\":1,\"b\":2}"),
            Map.of());
  }

  @Test
  void responseLostAfterCommitRetainsIdentityAndAcceptanceTime() {
    var first = service.ingest(event, null);
    var second = service.ingest(event, null);
    assertThat(first.duplicate()).isFalse();
    assertThat(second.duplicate()).isTrue();
    // PostgreSQL timestamps retain microseconds.
    assertThat(second.acceptedAt())
        .isEqualTo(first.acceptedAt().truncatedTo(java.time.temporal.ChronoUnit.MICROS));
    assertThat(count("ingested_event")).isEqualTo(1);
    assertThat(count("ingestion_outbox")).isEqualTo(1);
  }

  @Test
  void failedOutboxInsertRollsBackAcceptedEvent() {
    jdbc.sql("ALTER TABLE ingestion_outbox ADD CONSTRAINT inject_failure CHECK (topic='never')")
        .update();
    assertThatThrownBy(() -> service.ingest(event, null)).isInstanceOf(RuntimeException.class);
    assertThat(count("ingested_event")).isZero();
    assertThat(count("ingestion_outbox")).isZero();
  }

  @Test
  void canonicalDuplicateAndConflict() throws Exception {
    service.ingest(event, null);
    var reordered =
        new EventSubmission(
            event.eventId(),
            event.eventType(),
            event.source(),
            1,
            event.occurredAt(),
            mapper.readTree("{\"b\":2,\"a\":1}"),
            Map.of());
    assertThat(service.ingest(reordered, null).duplicate()).isTrue();
    var changed =
        new EventSubmission(
            event.eventId(),
            event.eventType(),
            event.source(),
            1,
            event.occurredAt(),
            mapper.readTree("{\"b\":3}"),
            Map.of());
    assertThatThrownBy(() -> service.ingest(changed, null))
        .isInstanceOf(IdempotencyConflictException.class);
  }

  @Test
  void crashAfterPublishCanRepublishButStaleOwnerCannotFinalizeOrRelease() {
    service.ingest(event, null);
    var old = outbox.claim(1, "old-token", 60).getFirst();
    // The broker accepted the payload, but the relay crashed before recording confirmation.
    jdbc.sql("UPDATE ingestion_outbox SET claimed_until=now() - interval '1 second'").update();
    var current = outbox.claim(1, "new-token", 60).getFirst();
    assertThat(current.payload()).isEqualTo(old.payload());
    assertThat(outbox.markPublished(old.id(), "old-token")).isFalse();
    outbox.release(old.id(), "old-token", "late failure");
    assertThat(outbox.claim(1, "third-token", 60)).isEmpty();
    assertThat(outbox.markPublished(current.id(), "new-token")).isTrue();
    assertThat(repository.findStatus(event.eventId()).orElseThrow().status())
        .isEqualTo("PUBLISHED");
    assertThat(outbox.claim(1, "next-token", 60)).isEmpty();
  }

  @Test
  void concurrentDuplicateSubmissionsCommitOneOutbox() throws Exception {
    try (var executor = Executors.newFixedThreadPool(2)) {
      var a = executor.submit(() -> service.ingest(event, null));
      var b = executor.submit(() -> service.ingest(event, null));
      assertThat(a.get().duplicate()).isNotEqualTo(b.get().duplicate());
    }
    assertThat(count("ingested_event")).isEqualTo(1);
    assertThat(count("ingestion_outbox")).isEqualTo(1);
  }
}
