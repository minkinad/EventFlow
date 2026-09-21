package io.github.minkin.eventflow.security;

import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;

/** Only validated authentication supplies tenant and subject; request fields never do. */
public record Caller(String tenant, String subject, boolean operator) {
  public static Caller current() {
    var authentication = SecurityContextHolder.getContext().getAuthentication();
    if (!(authentication instanceof JwtAuthenticationToken token) || !token.isAuthenticated()) {
      throw new org.springframework.security.access.AccessDeniedException(
          "JWT authentication required");
    }
    boolean operator =
        token.getAuthorities().stream()
            .anyMatch(
                a ->
                    java.util.Set.of("ROLE_ADMIN", "ROLE_VIEWER", "ROLE_DLQ_OPERATOR")
                        .contains(a.getAuthority()));
    return new Caller(token.getToken().getClaimAsString("tenant_id"), token.getName(), operator);
  }

  public String ownerFilter() {
    return operator ? null : subject;
  }

  public static String auditActor() {
    var authentication = SecurityContextHolder.getContext().getAuthentication();
    return authentication == null ? "system" : authentication.getName();
  }
}
