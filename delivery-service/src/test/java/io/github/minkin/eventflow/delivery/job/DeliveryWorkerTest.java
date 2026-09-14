package io.github.minkin.eventflow.delivery.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.minkin.eventflow.contracts.DeliveryTarget;
import io.github.minkin.eventflow.contracts.TargetType;
import io.github.minkin.eventflow.delivery.sink.DeliveryAdapter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class DeliveryWorkerTest {
  @Test
  void crashRecoveryBudgetPreventsUnlimitedExternalEffects() {
    var repository = mock(DeliveryJobRepository.class);
    var adapter = mock(DeliveryAdapter.class);
    when(adapter.targetType()).thenReturn(TargetType.POSTGRES);
    var job =
        new DeliveryJob(
            UUID.randomUUID(),
            null,
            new DeliveryTarget(TargetType.POSTGRES, "events", Map.of()),
            9,
            UUID.randomUUID());
    when(repository.claim(anyInt(), anyString(), anyInt())).thenReturn(List.of(job), List.of());
    when(repository.renew(job, 60)).thenReturn(true);
    var worker =
        new DeliveryWorker(
            repository,
            List.of(adapter),
            new SimpleMeterRegistry(),
            new DeliveryFailureClassifier(),
            10,
            8,
            60);
    worker.deliverBatch();
    verify(repository)
        .dead(job, "DELIVERY_ATTEMPTS_EXHAUSTED", "Recovery attempt budget exhausted");
    org.mockito.Mockito.verify(adapter, org.mockito.Mockito.never()).deliver(any());
    for (int i = 0; i < 100; i++) {
      assertThat(worker.backoffSeconds(i)).isBetween(1L, 300L);
    }
  }

  @Test
  void expiredBatchItemDoesNotCallSink() {
    var repository = mock(DeliveryJobRepository.class);
    var adapter = mock(DeliveryAdapter.class);
    when(adapter.targetType()).thenReturn(TargetType.POSTGRES);
    var job =
        new DeliveryJob(
            UUID.randomUUID(),
            null,
            new DeliveryTarget(TargetType.POSTGRES, "events", Map.of()),
            1,
            UUID.randomUUID());
    when(repository.claim(anyInt(), anyString(), anyInt())).thenReturn(List.of(job), List.of());
    var worker =
        new DeliveryWorker(
            repository,
            List.of(adapter),
            new SimpleMeterRegistry(),
            new DeliveryFailureClassifier(),
            10,
            8,
            60);
    worker.deliverBatch();
    org.mockito.Mockito.verify(adapter, org.mockito.Mockito.never()).deliver(any());
  }
}
