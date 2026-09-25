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
    var caller = io.github.minkin.eventflow.security.Caller.current();
    RedisTokenBucket.Decision decision =
        tokenBucket.consume(hash(caller.tenant()), hash(caller.subject()));
    if (decision.degraded()) {
      response.setHeader("X-RateLimit-Degraded", "true");
    }
    if (decision.remaining() >= 0) {
      response.setHeader("X-RateLimit-Remaining", Long.toString(decision.remaining()));
    }
    if (!decision.allowed()) {
      int status = decision.degraded() ? 503 : 429;
      response.setStatus(status);
      response.setHeader("Retry-After", "1");
      response.setContentType(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
      response
          .getWriter()
          .write(
              "{\"type\":\"about:blank\",\"title\":\""
                  + (decision.degraded() ? "Rate limiter unavailable" : "Rate limit exceeded")
                  + "\",\"status\":"
                  + status
                  + "}");
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
