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
      local time = redis.call('TIME')
      local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
      local tokens = {}
      local allowed = 1
      for i = 1, 2 do
        local capacity = tonumber(ARGV[(i-1)*2+1])
        local refill = tonumber(ARGV[(i-1)*2+2])
        local values = redis.call('HMGET', KEYS[i], 'tokens', 'updated')
        local previous = tonumber(values[1]) or capacity
        local updated = tonumber(values[2]) or now
        tokens[i] = math.min(capacity, previous + math.max(0, now-updated) / 1000 * refill)
        if tokens[i] < 1 then allowed = 0 end
      end
      for i = 1, 2 do
        tokens[i] = tokens[i] - allowed
        redis.call('HSET', KEYS[i], 'tokens', tokens[i], 'updated', now)
        redis.call('PEXPIRE', KEYS[i], ARGV[5])
      end
      return {allowed, math.floor(math.min(tokens[1], tokens[2]))}
      """;

  private final StringRedisTemplate redis;

  @SuppressWarnings("rawtypes")
  private final DefaultRedisScript<List> script = new DefaultRedisScript<>(SCRIPT, List.class);

  private final int capacity;
  private final int refillPerSecond;
  private final boolean failOpen;
  private final int tenantCapacity;
  private final int tenantRefill;
  private final io.micrometer.core.instrument.MeterRegistry metrics;

  public RedisTokenBucket(
      StringRedisTemplate redis,
      @Value("${eventflow.rate-limit.capacity:100}") int capacity,
      @Value("${eventflow.rate-limit.refill-per-second:10}") int refillPerSecond,
      @Value("${eventflow.rate-limit.fail-open:false}") boolean failOpen,
      @Value("${eventflow.rate-limit.tenant-capacity:1000}") int tenantCapacity,
      @Value("${eventflow.rate-limit.tenant-refill-per-second:100}") int tenantRefill,
      io.micrometer.core.instrument.MeterRegistry metrics) {
    this.redis = redis;
    this.capacity = capacity;
    this.refillPerSecond = refillPerSecond;
    if (capacity < 1 || refillPerSecond < 0 || tenantCapacity < 1 || tenantRefill < 0) {
      throw new IllegalArgumentException("Invalid token bucket configuration");
    }
    this.failOpen = failOpen;
    this.tenantCapacity = tenantCapacity;
    this.tenantRefill = tenantRefill;
    this.metrics = metrics;
  }

  public Decision consume(String tenantKey, String producerKey) {
    try {
      long ttlMillis =
          Math.max(
              60_000,
              Math.max(
                  capacity * 2_000L / Math.max(1, refillPerSecond),
                  tenantCapacity * 2_000L / Math.max(1, tenantRefill)));
      List<?> result =
          redis.execute(
              script,
              List.of(
                  "eventflow:rate:{" + tenantKey + "}:tenant",
                  "eventflow:rate:{" + tenantKey + "}:producer:" + producerKey),
              Integer.toString(tenantCapacity),
              Integer.toString(tenantRefill),
              Integer.toString(capacity),
              Integer.toString(refillPerSecond),
              Long.toString(ttlMillis));
      if (result == null || result.size() < 2) {
        throw new IllegalStateException("Redis returned no token bucket result");
      }
      metrics
          .counter(
              "eventflow.rate.limit.decisions",
              "outcome",
              ((Number) result.get(0)).longValue() == 1 ? "allowed" : "limited")
          .increment();
      return new Decision(
          ((Number) result.get(0)).longValue() == 1, ((Number) result.get(1)).longValue(), false);
    } catch (RuntimeException exception) {
      metrics
          .counter(
              "eventflow.rate.limit.decisions", "outcome", failOpen ? "fail_open" : "fail_closed")
          .increment();
      if (failOpen) {
        return new Decision(true, -1, true);
      }
      return new Decision(false, -1, true);
    }
  }

  public record Decision(boolean allowed, long remaining, boolean degraded) {}
}
