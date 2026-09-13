package io.github.minkin.eventflow.delivery.api;

import io.github.minkin.eventflow.delivery.job.DeliveryJobRepository;
import io.github.minkin.eventflow.delivery.job.DeliveryStatus;
import java.util.List;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/events")
public class DeliveryStatusController {
  private final DeliveryJobRepository repository;

  public DeliveryStatusController(DeliveryJobRepository repository) {
    this.repository = repository;
  }

  @GetMapping("/{eventId}/deliveries")
  ResponseEntity<List<DeliveryStatus>> statuses(@PathVariable UUID eventId) {
    List<DeliveryStatus> statuses = repository.findStatuses(eventId);
    return statuses.isEmpty() ? ResponseEntity.notFound().build() : ResponseEntity.ok(statuses);
  }
}
