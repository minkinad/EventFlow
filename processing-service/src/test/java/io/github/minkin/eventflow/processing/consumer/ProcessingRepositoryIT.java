package io.github.minkin.eventflow.processing.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.minkin.eventflow.contracts.DeadLetterEvent;
import io.github.minkin.eventflow.contracts.DeliveryCommand;
import io.github.minkin.eventflow.contracts.DeliveryTarget;
import io.github.minkin.eventflow.contracts.EventEnvelope;
import io.github.minkin.eventflow.contracts.TargetType;
import io.github.minkin.eventflow.processing.PostgresIntegrationSupport;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ProcessingRepositoryIT extends PostgresIntegrationSupport {
  private ProcessingRepository repository;
  private UUID eventId;
  private String raw;
  private EventEnvelope envelope;

  @BeforeEach
  void setup() throws Exception {
    repository = transactional(new ProcessingRepository(jdbc, mapper));
    eventId = UUID.randomUUID();
    envelope =
        new EventEnvelope(
            1,
            eventId,
            "order.created",
            "test",
            1,
            Instant.now(),
            Instant.now(),
            mapper.readTree("{\"orderId\":\"42\"}"),
            Map.of(),
            null);
    raw = mapper.writeValueAsString(envelope);
  }

  private ClaimResult claim() {
    return repository.claim(eventId, "a".repeat(64), raw, 60);
  }

  private DeliveryCommand command() {
    return new DeliveryCommand(
        1,
        UUID.randomUUID(),
        eventId,
        "order.created",
        "orders",
        1,
        envelope.payload(),
        List.of(new DeliveryTarget(TargetType.POSTGRES, "events", Map.of())),
        Map.of(),
        Instant.now(),
        null);
  }

  private DeadLetterEvent letter() throws Exception {
    return new DeadLetterEvent(
        1,
        UUID.randomUUID(),
        eventId,
        "PROCESSING",
        "INVALID",
        "invalid",
        1,
        mapper.readTree(raw),
        Instant.now(),
        null);
  }

  private void expire() {
    jdbc.sql("UPDATE processing_record SET lease_until=now() - interval '1 second'").update();
  }

  @Test
  void completionBeforeAckIsDeduplicatedWithExactlyOneOutbox() {
    var first = claim();
    repository.complete(eventId, first.leaseToken(), "orders", 1, command());
    assertThat(claim().decision()).isEqualTo(ClaimDecision.DUPLICATE);
    assertThat(count("processing_outbox")).isEqualTo(1);
  }

  @Test
  void expiredWorkerCannotCompleteRetryOrFailNewAttempt() throws Exception {
    var old = claim();
    expire();
    var current = claim();
    assertThat(current.attempt()).isEqualTo(2);
    assertThat(current.leaseToken()).isNotEqualTo(old.leaseToken());
    assertThatThrownBy(() -> repository.complete(eventId, old.leaseToken(), "orders", 1, command()))
        .isInstanceOf(LeaseLostException.class);
    assertThatThrownBy(
            () ->
                repository.releaseForRetry(
                    eventId, old.leaseToken(), Instant.now(), "TEMP", "error"))
        .isInstanceOf(LeaseLostException.class);
    assertThatThrownBy(() -> repository.fail(eventId, old.leaseToken(), "BAD", "error", letter()))
        .isInstanceOf(LeaseLostException.class);
    assertThat(count("processing_outbox")).isZero();
    assertThat(count("processing_dead_letter")).isZero();
    repository.complete(eventId, current.leaseToken(), "orders", 1, command());
  }

  @Test
  void expiryAloneFencesCompletionEvenWithoutAnotherWorker() {
    var first = claim();
    expire();
    assertThatThrownBy(
            () -> repository.complete(eventId, first.leaseToken(), "orders", 1, command()))
        .isInstanceOf(LeaseLostException.class);
  }

  @Test
  void failedRedeliveryIsTerminalAndReplayPreservesIdentityAndJsonbContent() throws Exception {
    var first = claim();
    var letter = letter();
    repository.fail(eventId, first.leaseToken(), "INVALID", "invalid", letter);
    assertThat(claim().decision()).isEqualTo(ClaimDecision.DUPLICATE);
    repository.replay(letter.deadLetterId());
    String replay =
        jdbc.sql(
                "SELECT payload::text FROM processing_outbox WHERE topic='eventflow.events.raw.v1'")
            .query(String.class)
            .single();
    var replayClaim = repository.claim(eventId, "b".repeat(64), replay, 60);
    assertThat(replayClaim.decision()).isEqualTo(ClaimDecision.CLAIMED);
    assertThat(replayClaim.attempt()).isEqualTo(1);
    repository.complete(eventId, replayClaim.leaseToken(), "orders", 1, command());
    assertThat(repository.findDeadLetterStatus(letter.deadLetterId())).contains("RESOLVED");
    assertThatThrownBy(() -> repository.replay(letter.deadLetterId()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void backoffSurvivesRepositoryRecreation() {
    var first = claim();
    repository.releaseForRetry(
        eventId, first.leaseToken(), Instant.now().plusSeconds(100), "TEMP", "error");
    repository = transactional(new ProcessingRepository(jdbc, mapper));
    assertThat(claim().decision()).isEqualTo(ClaimDecision.BUSY);
    jdbc.sql("UPDATE processing_record SET next_attempt_at=now() - interval '1 second'").update();
    assertThat(claim().attempt()).isEqualTo(2);
  }

  @Test
  void failedOutboxWriteRollsBackCompletion() {
    var first = claim();
    jdbc.sql("ALTER TABLE processing_outbox ADD CONSTRAINT inject_failure CHECK (topic='never')")
        .update();
    assertThatThrownBy(
            () -> repository.complete(eventId, first.leaseToken(), "orders", 1, command()))
        .isInstanceOf(RuntimeException.class);
    assertThat(repository.findStatus(eventId).orElseThrow().status()).isEqualTo("PROCESSING");
    assertThat(count("processing_outbox")).isZero();
  }

  @Test
  void rollbackBeforeClaimCommitLeavesNoInbox() {
    assertThatThrownBy(
            () ->
                transactions.executeWithoutResult(
                    status -> {
                      claim();
                      throw new IllegalStateException("crash");
                    }))
        .isInstanceOf(IllegalStateException.class);
    assertThat(count("processing_record")).isZero();
    assertThat(claim().decision()).isEqualTo(ClaimDecision.CLAIMED);
  }

  @Test
  void concurrentClaimHasOneWinner() throws Exception {
    try (var executor = Executors.newFixedThreadPool(2)) {
      var a = executor.submit(this::claim);
      var b = executor.submit(this::claim);
      assertThat(List.of(a.get().decision(), b.get().decision()))
          .containsExactlyInAnyOrder(ClaimDecision.CLAIMED, ClaimDecision.BUSY);
    }
  }

  @Test
  void changedContentCannotOverwriteIdentity() {
    claim();
    assertThat(repository.claim(eventId, "a".repeat(64), raw.replace("42", "43"), 60).decision())
        .isEqualTo(ClaimDecision.CONTENT_CONFLICT);
  }

  @Test
  void duplicateRejectionDoesNotMultiplyDlq() throws Exception {
    var letter = letter();
    repository.reject(letter);
    repository.reject(letter);
    assertThat(count("processing_dead_letter")).isEqualTo(1);
    assertThat(count("processing_outbox")).isEqualTo(1);
  }
}
