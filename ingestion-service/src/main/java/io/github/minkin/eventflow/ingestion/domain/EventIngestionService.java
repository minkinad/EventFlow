package io.github.minkin.eventflow.ingestion.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.minkin.eventflow.contracts.EventEnvelope;
import io.github.minkin.eventflow.contracts.EventTopics;
import io.github.minkin.eventflow.ingestion.api.EventSubmission;
import io.github.minkin.eventflow.ingestion.api.IngestionResponse;
import io.github.minkin.eventflow.ingestion.persistence.IngestionRepository;
import java.time.Clock;
import java.time.Instant;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class EventIngestionService {
  private final IngestionRepository repository;
  private final CanonicalJson canonicalJson;
  private final ObjectMapper objectMapper;
  private final Clock clock;

  @org.springframework.beans.factory.annotation.Autowired
  public EventIngestionService(
      IngestionRepository repository, CanonicalJson canonicalJson, ObjectMapper objectMapper) {
    this(repository, canonicalJson, objectMapper, Clock.systemUTC());
  }

  EventIngestionService(
      IngestionRepository repository,
      CanonicalJson canonicalJson,
      ObjectMapper objectMapper,
      Clock clock) {
    this.repository = repository;
    this.canonicalJson = canonicalJson;
    this.objectMapper = objectMapper;
    this.clock = clock;
  }

  @Transactional
  public IngestionResponse ingest(EventSubmission submission, String traceparent) {
    Instant acceptedAt = clock.instant().truncatedTo(java.time.temporal.ChronoUnit.MICROS);
    String contentHash = canonicalJson.sha256(objectMapper.valueToTree(submission));
    EventEnvelope envelope =
        new EventEnvelope(
            1,
            submission.eventId(),
            submission.eventType(),
            submission.source(),
            submission.schemaVersion(),
            submission.occurredAt(),
            acceptedAt,
            submission.payload(),
            submission.metadata(),
            traceparent);

    boolean inserted = repository.insertEvent(submission, acceptedAt, contentHash);
    if (!inserted) {
      String existingHash =
          repository
              .findContentHash(submission.eventId())
              .orElseThrow(() -> new IllegalStateException("Conflicting event disappeared"));
      if (!existingHash.equals(contentHash)) {
        throw new IdempotencyConflictException(submission.eventId());
      }
      Instant originalAcceptedAt =
          repository.findStatus(submission.eventId()).orElseThrow().receivedAt();
      return new IngestionResponse(submission.eventId(), "ACCEPTED", true, originalAcceptedAt);
    }

    try {
      repository.insertOutbox(
          submission.eventId(),
          EventTopics.RAW_EVENTS,
          objectMapper.writeValueAsString(envelope),
          acceptedAt);
    } catch (JsonProcessingException exception) {
      throw new IllegalArgumentException("Event cannot be serialized", exception);
    }
    return new IngestionResponse(submission.eventId(), "ACCEPTED", false, acceptedAt);
  }
}
