package io.github.minkin.eventflow.delivery.job;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import com.fasterxml.jackson.databind.json.JsonMapper;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

class DeliveryCommandListenerTest {
  private final DeliveryJobRepository repository = mock(DeliveryJobRepository.class);
  private final Acknowledgment ack = mock(Acknowledgment.class);
  private final DeliveryCommandListener listener =
      new DeliveryCommandListener(JsonMapper.builder().findAndAddModules().build(), repository);

  @Test
  void malformedRecordIsAcknowledgedOnlyAfterDurableRejection() {
    listener.consume("{}", ack);
    var order = inOrder(repository, ack);
    order.verify(repository).malformed(anyString(), anyString());
    order.verify(ack).acknowledge();
  }

  @Test
  void databaseOutageNeverAcknowledgesMalformedRecord() {
    doThrow(new IllegalStateException("database offline"))
        .when(repository)
        .malformed(anyString(), anyString());
    assertThatThrownBy(() -> listener.consume("broken", ack))
        .isInstanceOf(IllegalStateException.class);
    verifyNoInteractions(ack);
  }
}
