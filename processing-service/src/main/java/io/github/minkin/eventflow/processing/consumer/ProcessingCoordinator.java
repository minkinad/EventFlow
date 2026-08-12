package io.github.minkin.eventflow.processing.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.minkin.eventflow.contracts.DeadLetterEvent;
import io.github.minkin.eventflow.contracts.DeliveryCommand;
import io.github.minkin.eventflow.contracts.EventEnvelope;
import io.github.minkin.eventflow.processing.pipeline.PipelineException;
import io.github.minkin.eventflow.processing.pipeline.PipelineRepository;
import io.github.minkin.eventflow.processing.pipeline.PipelineEngine;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.UUID;

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

    public ProcessingCoordinator(ObjectMapper objectMapper, ProcessingRepository processingRepository,
                                 PipelineRepository pipelineRepository, PipelineEngine pipelineEngine,
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
        } catch (JsonProcessingException exception) {
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
            throw new ProcessingBusyException("Event processing lease is still active: " + event.eventId());
        }
        if (claim.decision() == ClaimDecision.CONTENT_CONFLICT) {
            reject(event, rawMessage, "EVENT_ID_CONTENT_CONFLICT", "eventId was reused with different content");
            return;
        }

        try {
            var pipeline = pipelineRepository.findActive(event.eventType())
                    .orElseThrow(() -> new PipelineException("PIPELINE_NOT_FOUND",
                            "No active pipeline for eventType " + event.eventType(), false));
            var result = pipelineEngine.execute(pipeline, event.payload());
            var command = new DeliveryCommand(1, UUID.randomUUID(), event.eventId(), event.eventType(),
                    pipeline.name(), pipeline.version(), result.payload(), result.targets(), event.metadata(),
                    Instant.now(), event.traceparent());
            processingRepository.complete(event.eventId(), pipeline.name(), pipeline.version(), command);
            succeeded.increment();
        } catch (PipelineException exception) {
            if (exception.retryable() && claim.attempt() < maxAttempts) {
                retry(event, exception.code(), exception.getMessage(), exception);
            } else {
                fail(event, rawMessage, exception.code(), exception.getMessage(), claim.attempt());
            }
        } catch (RuntimeException exception) {
            if (claim.attempt() < maxAttempts) {
                retry(event, "UNEXPECTED_PROCESSING_ERROR", exception.getMessage(), exception);
            } else {
                fail(event, rawMessage, "UNEXPECTED_PROCESSING_ERROR", exception.getMessage(), claim.attempt());
            }
        }
    }

    private void rejectMalformed(String raw, Exception exception) {
        UUID eventId = UUID.nameUUIDFromBytes(raw.getBytes(StandardCharsets.UTF_8));
        JsonNode original = objectMapper.createObjectNode().put("raw", raw);
        var deadLetter = new DeadLetterEvent(1, UUID.randomUUID(), eventId, "DESERIALIZATION",
                "MALFORMED_MESSAGE", exception.getMessage(), 1, original, Instant.now(), null);
        processingRepository.reject(deadLetter);
        failed.increment();
    }

    private void reject(EventEnvelope event, String raw, String code, String message) {
        processingRepository.reject(deadLetter(event, raw, code, message, 1));
        failed.increment();
    }

    private void retry(EventEnvelope event, String code, String message, RuntimeException cause) {
        processingRepository.releaseForRetry(event.eventId(), code, message);
        throw new RetryableProcessingException("Retryable processing failure for " + event.eventId(), cause);
    }

    private void fail(EventEnvelope event, String raw, String code, String message, int attempt) {
        processingRepository.fail(event.eventId(), code, message, deadLetter(event, raw, code, message, attempt));
        failed.increment();
    }

    private DeadLetterEvent deadLetter(EventEnvelope event, String raw, String code, String message, int attempt) {
        try {
            return new DeadLetterEvent(1, UUID.randomUUID(), event.eventId(), "PROCESSING", code, message, attempt,
                    objectMapper.readTree(raw), Instant.now(), event.traceparent());
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException(exception);
        }
    }

    private String sha256(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException exception) {
            throw new IllegalStateException(exception);
        }
    }
}
