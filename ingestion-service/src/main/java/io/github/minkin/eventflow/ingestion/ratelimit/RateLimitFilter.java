package io.github.minkin.eventflow.ingestion.ratelimit;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
public class RateLimitFilter extends OncePerRequestFilter {
  private final RedisTokenBucket tokenBucket;

  public RateLimitFilter(RedisTokenBucket tokenBucket) {
    this.tokenBucket = tokenBucket;
  }

  @Override
  protected boolean shouldNotFilter(HttpServletRequest request) {
    return !"POST".equals(request.getMethod()) || !"/api/v1/events".equals(request.getRequestURI());
  }

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws ServletException, IOException {
    String identity = request.getHeader("X-API-Key");
    if (identity == null || identity.isBlank()) {
      identity = request.getRemoteAddr();
    }
    RedisTokenBucket.Decision decision = tokenBucket.consume(hash(identity));
    if (decision.degraded()) {
      response.setHeader("X-RateLimit-Degraded", "true");
    }
    if (decision.remaining() >= 0) {
      response.setHeader("X-RateLimit-Remaining", Long.toString(decision.remaining()));
    }
    if (!decision.allowed()) {
      response.setStatus(429);
      response.setContentType(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
      response
          .getWriter()
          .write(
              "{\"type\":\"urn:eventflow:problem:rate-limit\","
                  + "\"title\":\"Rate limit exceeded\",\"status\":429}");
      return;
    }
    chain.doFilter(request, response);
  }

  private String hash(String value) {
    try {
      return HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException exception) {
      throw new IllegalStateException(exception);
    }
  }
}
