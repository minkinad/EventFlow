package io.github.minkin.eventflow.processing.api;

import io.github.minkin.eventflow.processing.consumer.ProcessingRepository;
import io.github.minkin.eventflow.processing.consumer.ProcessingStatus;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/events")
public class ProcessingStatusController {
  private final ProcessingRepository repository;

  public ProcessingStatusController(ProcessingRepository repository) {
    this.repository = repository;
  }

  @GetMapping("/{eventId}")
  ResponseEntity<ProcessingStatus> status(@PathVariable UUID eventId) {
    return repository
        .findStatus(eventId)
        .map(ResponseEntity::ok)
        .orElseGet(() -> ResponseEntity.notFound().build());
  }
}
