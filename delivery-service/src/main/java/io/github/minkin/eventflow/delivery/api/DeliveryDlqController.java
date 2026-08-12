package io.github.minkin.eventflow.delivery.api;

import io.github.minkin.eventflow.delivery.job.DeliveryJobRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/dlq")
public class DeliveryDlqController {
    private final DeliveryJobRepository repository;

    public DeliveryDlqController(DeliveryJobRepository repository) {
        this.repository = repository;
    }

    @PostMapping("/{id}/replay")
    ResponseEntity<Void> replay(@PathVariable UUID id) {
        repository.replay(id);
        return ResponseEntity.accepted().build();
    }
}
