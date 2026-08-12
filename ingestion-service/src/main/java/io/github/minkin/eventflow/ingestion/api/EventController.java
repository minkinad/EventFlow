package io.github.minkin.eventflow.ingestion.api;

import io.github.minkin.eventflow.ingestion.domain.EventIngestionService;
import io.github.minkin.eventflow.ingestion.persistence.IngestionRepository;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/events")
public class EventController {
    private final EventIngestionService ingestionService;
    private final IngestionRepository repository;

    public EventController(EventIngestionService ingestionService, IngestionRepository repository) {
        this.ingestionService = ingestionService;
        this.repository = repository;
    }

    @PostMapping
    public ResponseEntity<IngestionResponse> ingest(
            @Valid @RequestBody EventSubmission submission,
            @RequestHeader(name = "traceparent", required = false) String traceparent) {
        var result = ingestionService.ingest(submission, traceparent);
        return ResponseEntity.accepted()
                .location(URI.create("/api/v1/events/" + result.eventId()))
                .body(result);
    }

    @GetMapping("/{eventId}")
    public ResponseEntity<IngestionStatus> status(@PathVariable UUID eventId) {
        return repository.findStatus(eventId).map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
