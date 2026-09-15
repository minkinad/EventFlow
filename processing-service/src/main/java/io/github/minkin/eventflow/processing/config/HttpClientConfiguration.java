package io.github.minkin.eventflow.processing.config;

import java.net.http.HttpClient;
import java.time.Duration;
import org.springframework.boot.web.client.RestClientCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;

@Configuration
public class HttpClientConfiguration {
  @Bean
  RestClientCustomizer boundedHttpRequests() {
    var client =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();
    var factory = new JdkClientHttpRequestFactory(client);
    factory.setReadTimeout(Duration.ofSeconds(10));
    return builder -> builder.requestFactory(factory);
  }
}
