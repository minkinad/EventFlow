package io.github.minkin.eventflow.contracts;

public final class EventTopics {
  public static final String RAW_EVENTS = "eventflow.events.raw.v2";
  public static final String DELIVERY_COMMANDS = "eventflow.events.delivery.v2";
  public static final String PROCESSING_DLQ = "eventflow.events.processing-dlq.v2";
  public static final String DELIVERY_DLQ = "eventflow.events.delivery-dlq.v2";

  private EventTopics() {}
}
