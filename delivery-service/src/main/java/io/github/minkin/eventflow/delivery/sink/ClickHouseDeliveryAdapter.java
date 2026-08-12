package io.github.minkin.eventflow.delivery.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.minkin.eventflow.contracts.TargetType;
import io.github.minkin.eventflow.delivery.job.DeliveryJob;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
public class ClickHouseDeliveryAdapter implements DeliveryAdapter {
    private final RestClient client;
    private final ObjectMapper objectMapper;

    public ClickHouseDeliveryAdapter(RestClient.Builder builder, ObjectMapper objectMapper,
                                     @Value("${eventflow.clickhouse.url:http://localhost:8123}") String url,
                                     @Value("${eventflow.clickhouse.username:eventflow}") String username,
                                     @Value("${eventflow.clickhouse.password:eventflow}") String password) {
        this.client = builder.baseUrl(url).defaultHeaders(headers -> headers.setBasicAuth(username, password)).build();
        this.objectMapper = objectMapper;
    }

    @Override
    public TargetType targetType() {
        return TargetType.CLICKHOUSE;
    }

    @Override
    public void deliver(DeliveryJob job) {
        ObjectNode row = objectMapper.createObjectNode();
        row.put("event_id", job.command().eventId().toString());
        row.put("event_type", job.command().eventType());
        row.put("pipeline", job.command().pipeline());
        row.put("destination", job.target().destination());
        row.put("processed_at", job.command().processedAt().toString());
        row.put("version", job.command().processedAt().toEpochMilli());
        try {
            row.put("payload", objectMapper.writeValueAsString(job.command().payload()));
            client.post()
                    .uri("/?query=INSERT%20INTO%20eventflow.events%20FORMAT%20JSONEachRow")
                    .contentType(MediaType.APPLICATION_NDJSON)
                    .body(objectMapper.writeValueAsString(row) + "\n")
                    .retrieve().toBodilessEntity();
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("ClickHouse row cannot be serialized", exception);
        }
    }
}
