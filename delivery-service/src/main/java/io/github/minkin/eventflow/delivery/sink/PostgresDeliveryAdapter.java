package io.github.minkin.eventflow.delivery.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.minkin.eventflow.contracts.TargetType;
import io.github.minkin.eventflow.delivery.job.DeliveryJob;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

@Component
public class PostgresDeliveryAdapter implements DeliveryAdapter {
    private final JdbcClient jdbc;
    private final ObjectMapper objectMapper;

    public PostgresDeliveryAdapter(JdbcClient jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    @Override
    public TargetType targetType() {
        return TargetType.POSTGRES;
    }

    @Override
    public void deliver(DeliveryJob job) {
        jdbc.sql("""
                        INSERT INTO operational_event
                            (event_id, destination, event_type, pipeline, payload, processed_at, delivered_at)
                        VALUES (:eventId, :destination, :eventType, :pipeline,
                                CAST(:payload AS jsonb), :processedAt, now())
                        ON CONFLICT (event_id, destination) DO UPDATE
                        SET payload=EXCLUDED.payload, processed_at=EXCLUDED.processed_at, delivered_at=now()
                        """).param("eventId", job.command().eventId()).param("destination", job.target().destination())
                .param("eventType", job.command().eventType()).param("pipeline", job.command().pipeline())
                .param("payload", json(job.command().payload())).param("processedAt", job.command().processedAt()).update();
    }

    private String json(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException(exception);
        }
    }
}
