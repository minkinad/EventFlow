package io.github.minkin.eventflow.processing.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.minkin.eventflow.contracts.DeadLetterEvent;
import io.github.minkin.eventflow.contracts.DeliveryCommand;
import io.github.minkin.eventflow.contracts.EventEnvelope;
import io.github.minkin.eventflow.processing.pipeline.PipelineEngine;
import io.github.minkin.eventflow.processing.pipeline.PipelineException;
import io.github.minkin.eventflow.processing.pipeline.PipelineRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class ProcessingCoordinator {
  private final ObjectMapper objectMapper;
  private final ProcessingRepository processingRepository;
  private final PipelineRepository pipelineRepository;
  private final PipelineEngine pipelineEngine;
  private final int leaseSeconds;
  private final int maxAttempts;
  private final Counter succeeded;
  private final Counter failed;
  private final Counter duplicates;

  public ProcessingCoordinator(
      ObjectMapper objectMapper,
      ProcessingRepository processingRepository,
      PipelineRepository pipelineRepository,
      PipelineEngine pipelineEngine,
      MeterRegistry registry,
      @Value("${eventflow.processing.lease-seconds:120}") int leaseSeconds,
      @Value("${eventflow.processing.max-attempts:5}") int maxAttempts) {
    this.objectMapper = objectMapper;
    this.processingRepository = processingRepository;
    this.pipelineRepository = pipelineRepository;
    this.pipelineEngine = pipelineEngine;
    this.leaseSeconds = leaseSeconds;
    this.maxAttempts = maxAttempts;
    this.succeeded = registry.counter("eventflow.processing.completed");
    this.failed = registry.counter("eventflow.processing.failed");
    this.duplicates = registry.counter("eventflow.processing.duplicates");
  }

  public void handle(String rawMessage) {
    EventEnvelope event;
    try {
      event = objectMapper.readValue(rawMessage, EventEnvelope.class);
      io.github.minkin.eventflow.contracts.ContractValidation.validate(event);
    } catch (JsonProcessingException | IllegalArgumentException exception) {
      rejectMalformed(rawMessage, exception);
      return;
    }
    String hash = sha256(rawMessage);
    ClaimResult claim = processingRepository.claim(event.eventId(), hash, rawMessage, leaseSeconds);
    if (claim.decision() == ClaimDecision.DUPLICATE) {
      duplicates.increment();
      return;
    }
    if (claim.decision() == ClaimDecision.BUSY) {
      throw new ProcessingBusyException(
          "Event processing lease is still active: " + event.eventId());
    }
    if (claim.decision() == ClaimDecision.CONTENT_CONFLICT) {
      reject(
          event,
          rawMessage,
          "EVENT_ID_CONTENT_CONFLICT",
          "eventId was reused with different content");
      return;
    }

    if (claim.attempt() > maxAttempts) {
      fail(
          event,
          claim,
          rawMessage,
          "PROCESSING_ATTEMPTS_EXHAUSTED",
          "Recovery attempt budget exhausted",
          claim.attempt());
      return;
    }
    try {
      var pipeline =
          pipelineRepository
              .findActive(event.eventType())
              .orElseThrow(
                  () ->
                      new PipelineException(
                          "PIPELINE_NOT_FOUND",
                          "No active pipeline for eventType " + event.eventType(),
                          false));
      var result = pipelineEngine.execute(pipeline, event.payload());
      var command =
          new DeliveryCommand(
              1,
              UUID.randomUUID(),
              event.eventId(),
              event.eventType(),
              pipeline.name(),
              pipeline.version(),
              result.payload(),
              result.targets(),
              event.metadata(),
              Instant.now(),
              event.traceparent());
      processingRepository.complete(
          event.eventId(), claim.leaseToken(), pipeline.name(), pipeline.version(), command);
      succeeded.increment();
    } catch (LeaseLostException exception) {
      throw exception;
    } catch (PipelineException exception) {
      if (exception.retryable() && claim.attempt() < maxAttempts) {
        retry(event, claim, exception.code(), exception.getMessage(), exception);
      } else {
        fail(event, claim, rawMessage, exception.code(), exception.getMessage(), claim.attempt());
      }
    } catch (RuntimeException exception) {
      if (claim.attempt() < maxAttempts) {
        retry(event, claim, "UNEXPECTED_PROCESSING_ERROR", exception.getMessage(), exception);
      } else {
        fail(
            event,
            claim,
            rawMessage,
            "UNEXPECTED_PROCESSING_ERROR",
            exception.getMessage(),
            claim.attempt());
      }
    }
  }

  private void rejectMalformed(String raw, Exception exception) {
    UUID eventId = UUID.nameUUIDFromBytes(raw.getBytes(StandardCharsets.UTF_8));
    JsonNode original = objectMapper.createObjectNode().put("raw", raw);
    var deadLetter =
        new DeadLetterEvent(
            1,
            eventId,
            eventId,
            "DESERIALIZATION",
            "MALFORMED_MESSAGE",
            "Invalid or unsupported event contract",
            1,
            original,
            Instant.now(),
            null);
    processingRepository.reject(deadLetter);
    failed.increment();
  }

  private void reject(EventEnvelope event, String raw, String code, String message) {
    var rejected = deadLetter(event, raw, code, message, 1);
    processingRepository.reject(
        new DeadLetterEvent(
            1,
            UUID.nameUUIDFromBytes(("conflict:" + raw).getBytes(StandardCharsets.UTF_8)),
            event.eventId(),
            "CONTENT_CONFLICT",
            code,
            message,
            1,
            rejected.originalMessage(),
            Instant.now(),
            event.traceparent()));
    failed.increment();
  }

  private void retry(
      EventEnvelope event, ClaimResult claim, String code, String message, RuntimeException cause) {
    long ceiling = Math.min(300, 1L << Math.min(claim.attempt(), 9));
    long delay = java.util.concurrent.ThreadLocalRandom.current().nextLong(1, ceiling + 1);
    processingRepository.releaseForRetry(
        event.eventId(), claim.leaseToken(), Instant.now().plusSeconds(delay), code, message);
    throw new RetryableProcessingException(
        "Retryable processing failure for " + event.eventId(), cause);
  }

  private void fail(
      EventEnvelope event,
      ClaimResult claim,
      String raw,
      String code,
      String message,
      int attempt) {
    processingRepository.fail(
        event.eventId(),
        claim.leaseToken(),
        code,
        message,
        deadLetter(event, raw, code, message, attempt));
    failed.increment();
  }

  private DeadLetterEvent deadLetter(
      EventEnvelope event, String raw, String code, String message, int attempt) {
    try {
      return new DeadLetterEvent(
          1,
          UUID.randomUUID(),
          event.eventId(),
          "PROCESSING",
          code,
          message,
          attempt,
          objectMapper.readTree(raw),
          Instant.now(),
          event.traceparent());
    } catch (JsonProcessingException exception) {
      throw new IllegalStateException(exception);
    }
  }

  private String sha256(String value) {
    try {
      return HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
    } catch (java.security.NoSuchAlgorithmException exception) {
      throw new IllegalStateException(exception);
    }
  }
}
