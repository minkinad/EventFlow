package io.github.minkin.eventflow.processing.api;

import io.github.minkin.eventflow.processing.consumer.ProcessingRepository;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/dlq")
public class DeadLetterController {
  private final ProcessingRepository repository;

  public DeadLetterController(ProcessingRepository repository) {
    this.repository = repository;
  }

  @PostMapping("/{id}/replay")
  ResponseEntity<Void> replay(@PathVariable UUID id) {
    repository.replay(id);
    return ResponseEntity.accepted().build();
  }
}
