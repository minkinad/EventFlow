package io.github.minkin.eventflow.delivery.sink;

import io.github.minkin.eventflow.contracts.TargetType;
import io.github.minkin.eventflow.delivery.job.DeliveryJob;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
public class ExternalHttpDeliveryAdapter implements DeliveryAdapter {
    private final RestClient client;

    public ExternalHttpDeliveryAdapter(RestClient.Builder builder) {
        this.client = builder.build();
    }

    @Override
    public TargetType targetType() {
        return TargetType.EXTERNAL_HTTP;
    }

    @Override
    @CircuitBreaker(name = "externalDelivery")
    public void deliver(DeliveryJob job) {
        client.post().uri(job.target().destination())
                .contentType(MediaType.APPLICATION_JSON)
                .header("Idempotency-Key", job.command().eventId().toString())
                .body(job.command())
                .retrieve().toBodilessEntity();
    }
}
