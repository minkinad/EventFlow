package io.github.minkin.eventflow.security;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("eventflow.security")
public record SecurityProperties(
    @NotBlank String issuer, @NotBlank String jwkSetUri, @NotBlank String audience) {}
