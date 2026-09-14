package io.github.minkin.eventflow.delivery.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class KafkaConsumerConfiguration {
  @Bean
  DefaultErrorHandler deliveryErrorHandler() {
    ExponentialBackOff backOff = new ExponentialBackOff(500, 2.0);
    backOff.setMaxInterval(30_000);
    // Business failures are bounded in durable state; infrastructure failure cannot discard a
    // record.
    backOff.setMaxElapsedTime(Long.MAX_VALUE);
    var handler =
        new DefaultErrorHandler(
            (record, exception) -> {
              throw new IllegalStateException(
                  "No durable outcome; Kafka record must remain uncommitted", exception);
            },
            backOff);
    handler.setClassifications(java.util.Map.of(), true);
    handler.setAckAfterHandle(false);
    return handler;
  }
}
