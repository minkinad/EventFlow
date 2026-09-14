package io.github.minkin.eventflow.ingestion.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class RedisTokenBucketIT {
  @Container
  static final GenericContainer<?> REDIS =
      new GenericContainer<>("redis:7.4-alpine").withExposedPorts(6379);

  @Test
  void concurrentCallsCannotSpendSameToken() throws Exception {
    var connection = new LettuceConnectionFactory(REDIS.getHost(), REDIS.getMappedPort(6379));
    connection.afterPropertiesSet();
    connection.start();
    try (var executor = Executors.newFixedThreadPool(8)) {
      var bucket = new RedisTokenBucket(new StringRedisTemplate(connection), 1, 0, true);
      String key = UUID.randomUUID().toString();
      var requests =
          new java.util.ArrayList<java.util.concurrent.Future<RedisTokenBucket.Decision>>();
      for (int i = 0; i < 20; i++) {
        requests.add(executor.submit(() -> bucket.consume(key)));
      }
      int accepted = 0;
      for (var request : requests) {
        var result = request.get();
        assertThat(result.degraded()).isFalse();
        if (result.allowed()) {
          accepted++;
        }
      }
      assertThat(accepted).isEqualTo(1);
    } finally {
      connection.destroy();
    }
  }
}
