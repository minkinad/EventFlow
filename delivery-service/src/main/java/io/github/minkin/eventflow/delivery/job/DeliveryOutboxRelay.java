package io.github.minkin.eventflow.delivery.job;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
public class DeliveryOutboxRelay {
    private final JdbcClient jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final String owner = UUID.randomUUID().toString();
    private final int batchSize;
    private final TransactionTemplate transactionTemplate;

    public DeliveryOutboxRelay(JdbcClient jdbc, KafkaTemplate<String, String> kafka,
                               PlatformTransactionManager transactionManager,
                               @Value("${eventflow.outbox.batch-size:100}") int batchSize) {
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.batchSize = batchSize;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @Scheduled(fixedDelayString = "${eventflow.outbox.poll-interval:500ms}")
    public void publish() {
        for (Message message : claimInTransaction()) {
            try {
                kafka.send(message.topic(), message.key(), message.payload()).get(10, TimeUnit.SECONDS);
                jdbc.sql("""
                                UPDATE delivery_outbox SET published_at=now(), claimed_by=NULL, claimed_until=NULL
                                WHERE id=:id
                                """).param("id", message.id()).update();
            } catch (Exception exception) {
                String error = exception.getMessage() == null ? "unknown publication failure" : exception.getMessage();
                jdbc.sql("""
                                UPDATE delivery_outbox SET claimed_by=NULL, claimed_until=NULL, last_error=:error
                                WHERE id=:id
                                """).param("id", message.id())
                        .param("error", error.substring(0, Math.min(error.length(), 2000))).update();
            }
        }
    }

    private List<Message> claimInTransaction() {
        List<Message> claimed = transactionTemplate.execute(status -> jdbc.sql("""
                        WITH candidates AS (
                            SELECT id FROM delivery_outbox
                            WHERE published_at IS NULL AND (claimed_until IS NULL OR claimed_until < now())
                            ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT :limit
                        )
                        UPDATE delivery_outbox o SET claimed_by=:owner,
                            claimed_until=now() + interval '30 seconds', attempts=attempts+1
                        FROM candidates c WHERE o.id=c.id
                        RETURNING o.id, o.topic, o.message_key, o.payload::text
                        """).param("limit", batchSize).param("owner", owner)
                .query((rs, rowNum) -> new Message(rs.getObject("id", UUID.class), rs.getString("topic"),
                        rs.getString("message_key"), rs.getString("payload")))
                .list());
        return claimed == null ? List.of() : claimed;
    }

    record Message(UUID id, String topic, String key, String payload) {
    }
}
