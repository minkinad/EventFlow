package io.github.minkin.eventflow.delivery.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.minkin.eventflow.contracts.DeliveryCommand;
import io.github.minkin.eventflow.contracts.DeliveryTarget;
import io.github.minkin.eventflow.contracts.TargetType;
import io.github.minkin.eventflow.delivery.PostgresIntegrationSupport;
import io.github.minkin.eventflow.delivery.sink.PostgresDeliveryAdapter;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DeliveryJobRepositoryIT extends PostgresIntegrationSupport {
  private DeliveryJobRepository repository;
  private DeliveryCommand command;
  private String raw;

  @BeforeEach
  void setup() throws Exception {
    repository = transactional(new DeliveryJobRepository(jdbc, mapper));
    command =
        new DeliveryCommand(
            1,
            UUID.randomUUID(),
            UUID.randomUUID(),
            "order.created",
            "orders",
            1,
            mapper.readTree("{\"orderId\":\"42\"}"),
            List.of(new DeliveryTarget(TargetType.POSTGRES, "events", Map.of())),
            Map.of(),
            Instant.now(),
            null);
    raw = mapper.writeValueAsString(command);
  }

  private DeliveryJob claim() {
    return repository.claim(1, "worker", 60).getFirst();
  }

  private void expire() {
    jdbc.sql("UPDATE delivery_job SET lease_until=now() - interval '1 second'").update();
  }

  private UUID dlqId() {
    return jdbc.sql("SELECT id FROM delivery_dead_letter ORDER BY failed_at DESC LIMIT 1")
        .query(UUID.class)
        .single();
  }

  @Test
  void duplicateCommandCreatesOneJobAndChangedContentIsRejected() {
    assertThat(repository.accept(command, raw)).isTrue();
    assertThat(repository.accept(command, raw)).isFalse();
    assertThat(repository.accept(command, raw.replace("42", "43"))).isFalse();
    assertThat(count("delivery_job")).isEqualTo(1);
    assertThat(count("delivery_dead_letter")).isEqualTo(1);
    assertThat(claim().attempt()).isEqualTo(1);
  }

  @Test
  void crashBeforeSinkCallIsRecoveredAndOldWorkerIsFenced() {
    repository.accept(command, raw);
    var old = claim();
    expire();
    var current = claim();
    assertThat(current.id()).isEqualTo(old.id());
    assertThat(current.leaseToken()).isNotEqualTo(old.leaseToken());
    assertThat(current.attempt()).isEqualTo(2);
    assertThat(repository.renew(old, 60)).isFalse();
    assertThatThrownBy(() -> repository.succeeded(old)).isInstanceOf(LeaseLostException.class);
    assertThatThrownBy(() -> repository.retry(old, Instant.now(), "old failure"))
        .isInstanceOf(LeaseLostException.class);
    assertThatThrownBy(() -> repository.dead(old, "OLD", "old failure"))
        .isInstanceOf(LeaseLostException.class);
    assertThat(count("delivery_dead_letter")).isZero();
    repository.succeeded(current);
    assertThat(repository.findStatuses(command.eventId()).getFirst().status())
        .isEqualTo("SUCCEEDED");
  }

  @Test
  void crashAfterSinkCommitRepeatsCallWithOneBusinessRow() {
    repository.accept(command, raw);
    var first = claim();
    var sink = new PostgresDeliveryAdapter(jdbc, mapper);
    sink.deliver(first);
    expire();
    var second = claim();
    sink.deliver(second);
    repository.succeeded(second);
    assertThat(count("operational_event")).isEqualTo(1);
    assertThat(repository.claim(1, "worker", 60)).isEmpty();
  }

  @Test
  void retryScheduleIsDurableAndReplayResolvesOnlyAfterSuccess() {
    repository.accept(command, raw);
    var first = claim();
    repository.retry(first, Instant.now().plusSeconds(300), "outage");
    repository = transactional(new DeliveryJobRepository(jdbc, mapper));
    assertThat(repository.claim(1, "worker", 60)).isEmpty();
    jdbc.sql("UPDATE delivery_job SET next_attempt_at=now()").update();
    var second = claim();
    repository.dead(second, "DELIVERY_ATTEMPTS_EXHAUSTED", "outage");
    var id = dlqId();
    repository.replay(id);
    assertThat(
            jdbc.sql("SELECT status FROM delivery_dead_letter WHERE id=:id")
                .param("id", id)
                .query(String.class)
                .single())
        .isEqualTo("REPLAYED");
    var replay = claim();
    assertThat(replay.command().eventId()).isEqualTo(command.eventId());
    assertThat(replay.attempt()).isEqualTo(1);
    repository.succeeded(replay);
    assertThat(
            jdbc.sql("SELECT status FROM delivery_dead_letter WHERE id=:id")
                .param("id", id)
                .query(String.class)
                .single())
        .isEqualTo("RESOLVED");
    assertThatThrownBy(() -> repository.replay(id)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void dlqOutboxFailureRollsBackDeadTransition() {
    repository.accept(command, raw);
    var job = claim();
    jdbc.sql("ALTER TABLE delivery_outbox ADD CONSTRAINT inject_failure CHECK (topic='never')")
        .update();
    assertThatThrownBy(() -> repository.dead(job, "DEAD", "error"))
        .isInstanceOf(RuntimeException.class);
    assertThat(repository.findStatuses(command.eventId()).getFirst().status())
        .isEqualTo("DELIVERING");
    assertThat(count("delivery_dead_letter")).isZero();
  }

  @Test
  void failedJobInsertRollsBackInbox() {
    jdbc.sql("ALTER TABLE delivery_job ADD CONSTRAINT inject_failure CHECK (destination='never')")
        .update();
    assertThatThrownBy(() -> repository.accept(command, raw)).isInstanceOf(RuntimeException.class);
    assertThat(count("delivery_message")).isZero();
  }

  @Test
  void concurrentWorkersCannotClaimSameJob() throws Exception {
    repository.accept(command, raw);
    try (var executor = Executors.newFixedThreadPool(2)) {
      var a = executor.submit(() -> repository.claim(1, "a", 60));
      var b = executor.submit(() -> repository.claim(1, "b", 60));
      assertThat(a.get().size() + b.get().size()).isEqualTo(1);
    }
  }

  @Test
  void malformedRedeliveryHasOneDurableRejectionAndCannotReplay() {
    repository.malformed("broken", "invalid");
    repository.malformed("broken", "invalid");
    assertThat(count("delivery_dead_letter")).isEqualTo(1);
    assertThat(count("delivery_outbox")).isEqualTo(1);
    assertThatThrownBy(() -> repository.replay(dlqId()))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
