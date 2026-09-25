package io.github.minkin.eventflow.ingestion.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;

class RateLimitFailureTest {
  @AfterEach
  void clear() {
    SecurityContextHolder.clearContext();
  }

  @Test
  void redisOutageHasExplicitAdmissionPolicyAndNoInternalErrorLeak() throws Exception {
    var jwt =
        Jwt.withTokenValue("test")
            .header("alg", "RS256")
            .subject("producer")
            .claim("tenant_id", "demo")
            .build();
    SecurityContextHolder.getContext()
        .setAuthentication(
            new JwtAuthenticationToken(jwt, List.of(new SimpleGrantedAuthority("ROLE_PRODUCER"))));
    var unavailable =
        new StringRedisTemplate() {
          @Override
          public <T> T execute(RedisScript<T> script, List<String> keys, Object... args) {
            throw new org.springframework.data.redis.RedisConnectionFailureException(
                "internal-hostname:6379");
          }
        };
    for (boolean failOpen : List.of(false, true)) {
      var metrics = new SimpleMeterRegistry();
      var filter =
          new RateLimitFilter(
              new RedisTokenBucket(unavailable, 100, 10, failOpen, 1000, 100, metrics));
      var request = new MockHttpServletRequest("POST", "/api/v1/events");
      request.addHeader("X-API-Key", "untrusted-bucket");
      var response = new MockHttpServletResponse();
      var continued = new java.util.concurrent.atomic.AtomicBoolean();
      filter.doFilter(request, response, (req, res) -> continued.set(true));
      assertThat(continued.get()).isEqualTo(failOpen);
      assertThat(response.getHeader("X-RateLimit-Degraded")).isEqualTo("true");
      assertThat(response.getContentAsString()).doesNotContain("internal-hostname");
      if (!failOpen) {
        assertThat(response.getStatus()).isEqualTo(503);
        assertThat(response.getHeader("Retry-After")).isEqualTo("1");
      }
      assertThat(
              metrics
                  .get("eventflow.rate.limit.decisions")
                  .tag("outcome", failOpen ? "fail_open" : "fail_closed")
                  .counter()
                  .count())
          .isEqualTo(1);
    }
  }
}
