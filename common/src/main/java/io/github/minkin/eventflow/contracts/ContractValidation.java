package io.github.minkin.eventflow.contracts;

/** Validation at the untrusted Kafka boundary, before any durable business claim. */
public final class ContractValidation {
  private ContractValidation() {}

  public static void validate(EventEnvelope event) {
    require(event != null, "Missing event");
    require(event.contractVersion() == 2, "Unsupported event contract version");
    text(event.tenantId(), 120, "tenantId");
    text(event.producerId(), 200, "producerId");
    require(event.eventId() != null, "Missing eventId");
    text(event.eventType(), 120, "eventType");
    text(event.source(), 120, "source");
    require(event.schemaVersion() > 0, "Invalid schema version");
    require(event.occurredAt() != null && event.receivedAt() != null, "Missing event timestamps");
    require(event.payload() != null && !event.payload().isNull(), "Missing event payload");
  }

  public static void validate(DeliveryCommand command) {
    require(command != null, "Missing delivery command");
    require(command.contractVersion() == 2, "Unsupported delivery contract version");
    text(command.tenantId(), 120, "tenantId");
    text(command.producerId(), 200, "producerId");
    require(command.commandId() != null && command.eventId() != null, "Missing delivery identity");
    text(command.eventType(), 120, "eventType");
    text(command.pipeline(), 120, "pipeline");
    require(command.pipelineVersion() > 0, "Invalid pipeline version");
    require(command.processedAt() != null, "Missing processing timestamp");
    require(command.payload() != null && !command.payload().isNull(), "Missing delivery payload");
    require(
        !command.targets().isEmpty() && command.targets().size() <= 32, "Invalid delivery targets");
    var routes = new java.util.HashSet<String>();
    for (DeliveryTarget target : command.targets()) {
      require(target != null && target.type() != null, "Missing target type");
      text(target.destination(), 255, "destination");
      require(routes.add(target.type() + ":" + target.destination()), "Duplicate delivery route");
    }
  }

  private static void text(String value, int limit, String field) {
    require(value != null && !value.isBlank() && value.length() <= limit, "Invalid " + field);
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }
}
