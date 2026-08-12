package io.github.minkin.eventflow.contracts;

public final class EventTopics {
    public static final String RAW_EVENTS = "eventflow.events.raw.v1";
    public static final String DELIVERY_COMMANDS = "eventflow.events.delivery.v1";
    public static final String PROCESSING_DLQ = "eventflow.events.processing-dlq.v1";
    public static final String DELIVERY_DLQ = "eventflow.events.delivery-dlq.v1";

    private EventTopics() {
    }
}
