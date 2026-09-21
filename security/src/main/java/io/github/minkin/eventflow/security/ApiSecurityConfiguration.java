package io.github.minkin.eventflow.security;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.web.SecurityFilterChain;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(SecurityProperties.class)
public class ApiSecurityConfiguration {
  private static final Set<String> ROLES =
      Set.of("PRODUCER", "PIPELINE_EDITOR", "DLQ_OPERATOR", "VIEWER", "ADMIN");

  @Bean
  JwtDecoder jwtDecoder(SecurityProperties properties) {
    // Explicit JWKS endpoint avoids an issuer-discovery dependency during application startup.
    var decoder = NimbusJwtDecoder.withJwkSetUri(properties.jwkSetUri()).build();
    decoder.setJwtValidator(
        new DelegatingOAuth2TokenValidator<>(
            JwtValidators.createDefaultWithIssuer(properties.issuer()),
            jwt -> {
              Object tenant = jwt.getClaims().get("tenant_id");
              Object subject = jwt.getClaims().get("sub");
              boolean valid =
                  jwt.getAudience() != null
                      && jwt.getAudience().contains(properties.audience())
                      && tenant instanceof String t
                      && t.matches("[a-zA-Z0-9][a-zA-Z0-9._-]{0,119}")
                      && subject instanceof String s
                      && !s.isBlank()
                      && s.length() <= 200
                      && jwt.getExpiresAt() != null;
              return valid
                  ? OAuth2TokenValidatorResult.success()
                  : OAuth2TokenValidatorResult.failure(
                      new OAuth2Error("invalid_token", "Required token claims are invalid", null));
            }));
    return decoder;
  }

  @Bean
  SecurityFilterChain apiSecurity(HttpSecurity http) throws Exception {
    var converter = new JwtAuthenticationConverter();
    converter.setJwtGrantedAuthoritiesConverter(
        jwt -> {
          Object roles = jwt.getClaims().get("roles");
          if (!(roles instanceof Collection<?> values)) {
            return List.of();
          }
          return values.stream()
              .filter(String.class::isInstance)
              .map(String.class::cast)
              .filter(ROLES::contains)
              .distinct()
              .<org.springframework.security.core.GrantedAuthority>map(
                  role -> new SimpleGrantedAuthority("ROLE_" + role))
              .toList();
        });
    http.csrf(AbstractHttpConfigurer::disable)
        .cors(Customizer.withDefaults())
        .sessionManagement(
            session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
        .requestCache(AbstractHttpConfigurer::disable)
        .authorizeHttpRequests(
            auth ->
                auth.dispatcherTypeMatchers(jakarta.servlet.DispatcherType.ERROR)
                    .permitAll()
                    .requestMatchers(
                        HttpMethod.GET, "/actuator/health/liveness", "/actuator/health/readiness")
                    .permitAll()
                    .requestMatchers(HttpMethod.POST, "/api/v1/events")
                    .hasAnyRole("PRODUCER", "ADMIN")
                    .requestMatchers(HttpMethod.GET, "/api/v1/events/**")
                    .hasAnyRole("PRODUCER", "VIEWER", "DLQ_OPERATOR", "ADMIN")
                    .requestMatchers(HttpMethod.GET, "/api/v1/pipelines/**", "/api/v1/schemas/**")
                    .hasAnyRole("PIPELINE_EDITOR", "VIEWER", "ADMIN")
                    .requestMatchers(
                        "/api/v1/pipelines", "/api/v1/pipelines/**", "/api/v1/schemas/**")
                    .hasAnyRole("PIPELINE_EDITOR", "ADMIN")
                    .requestMatchers(HttpMethod.GET, "/api/v1/dlq/**")
                    .hasAnyRole("DLQ_OPERATOR", "VIEWER", "ADMIN")
                    .requestMatchers("/api/v1/dlq/**")
                    .hasAnyRole("DLQ_OPERATOR", "ADMIN")
                    .requestMatchers(HttpMethod.GET, "/actuator/prometheus")
                    .hasAnyRole("VIEWER", "ADMIN")
                    .anyRequest()
                    .denyAll())
        .oauth2ResourceServer(
            oauth ->
                oauth
                    .jwt(jwt -> jwt.jwtAuthenticationConverter(converter))
                    .authenticationEntryPoint(
                        (request, response, failure) ->
                            problem(response, 401, "Authentication required"))
                    .accessDeniedHandler(
                        (request, response, failure) -> problem(response, 403, "Access denied")))
        .exceptionHandling(
            errors ->
                errors
                    .authenticationEntryPoint(
                        (request, response, failure) ->
                            problem(response, 401, "Authentication required"))
                    .accessDeniedHandler(
                        (request, response, failure) -> problem(response, 403, "Access denied")));
    return http.build();
  }

  private static void problem(
      jakarta.servlet.http.HttpServletResponse response, int status, String title)
      throws java.io.IOException {
    response.setStatus(status);
    response.setContentType("application/problem+json");
    if (status == 401) {
      response.setHeader("WWW-Authenticate", "Bearer");
    }
    response
        .getWriter()
        .write("{\"type\":\"about:blank\",\"title\":\"" + title + "\",\"status\":" + status + "}");
  }
}
