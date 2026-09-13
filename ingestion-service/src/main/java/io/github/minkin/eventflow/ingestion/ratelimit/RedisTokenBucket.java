package io.github.minkin.eventflow.ingestion.ratelimit;

import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

@Component
public class RedisTokenBucket {
  private static final String SCRIPT =
      """
            local values = redis.call('HMGET', KEYS[1], 'tokens', 'updated')
            local tokens = tonumber(values[1]) or tonumber(ARGV[1])
            local updated = tonumber(values[2]) or tonumber(ARGV[3])
            local elapsed = math.max(0, tonumber(ARGV[3]) - updated) / 1000
            tokens = math.min(tonumber(ARGV[1]), tokens + elapsed * tonumber(ARGV[2]))
            local allowed = 0
            if tokens >= 1 then
              tokens = tokens - 1
              allowed = 1
            end
            redis.call('HSET', KEYS[1], 'tokens', tokens, 'updated', ARGV[3])
            redis.call('PEXPIRE', KEYS[1], ARGV[4])
            return {allowed, math.floor(tokens)}
            """;

  private final StringRedisTemplate redis;

  @SuppressWarnings("rawtypes")
  private final DefaultRedisScript<List> script = new DefaultRedisScript<>(SCRIPT, List.class);

  private final int capacity;
  private final int refillPerSecond;
  private final boolean failOpen;

  public RedisTokenBucket(
      StringRedisTemplate redis,
      @Value("${eventflow.rate-limit.capacity:100}") int capacity,
      @Value("${eventflow.rate-limit.refill-per-second:10}") int refillPerSecond,
      @Value("${eventflow.rate-limit.fail-open:true}") boolean failOpen) {
    this.redis = redis;
    this.capacity = capacity;
    this.refillPerSecond = refillPerSecond;
    this.failOpen = failOpen;
  }

  public Decision consume(String clientKey) {
    try {
      long ttlMillis = Math.max(60_000, capacity * 2_000L / Math.max(1, refillPerSecond));
      List<?> result =
          redis.execute(
              script,
              List.of("eventflow:rate:" + clientKey),
              Integer.toString(capacity),
              Integer.toString(refillPerSecond),
              Long.toString(System.currentTimeMillis()),
              Long.toString(ttlMillis));
      if (result == null || result.size() < 2) {
        throw new IllegalStateException("Redis returned no token bucket result");
      }
      return new Decision(
          ((Number) result.get(0)).longValue() == 1, ((Number) result.get(1)).longValue(), false);
    } catch (RuntimeException exception) {
      if (failOpen) {
        return new Decision(true, -1, true);
      }
      throw exception;
    }
  }

  public record Decision(boolean allowed, long remaining, boolean degraded) {}
}
