package io.github.minkin.eventflow.processing.consumer;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

class RawEventListenerTest {
  @Test
  void ackFollowsDurableOutcomeAndNeverFollowsInfrastructureFailure() {
    var coordinator = mock(ProcessingCoordinator.class);
    var ack = mock(Acknowledgment.class);
    var listener = new RawEventListener(coordinator);
    listener.consume("event", ack);
    var order = inOrder(coordinator, ack);
    order.verify(coordinator).handle("event");
    order.verify(ack).acknowledge();
    var uncommitted = mock(Acknowledgment.class);
    doThrow(new IllegalStateException("db offline")).when(coordinator).handle("other");
    assertThatThrownBy(() -> listener.consume("other", uncommitted))
        .isInstanceOf(IllegalStateException.class);
    verifyNoInteractions(uncommitted);
  }
}
