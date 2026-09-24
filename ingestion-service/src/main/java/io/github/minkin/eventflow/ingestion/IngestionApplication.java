package io.github.minkin.eventflow.ingestion;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@org.springframework.context.annotation.Import(
    io.github.minkin.eventflow.security.ApiSecurityConfiguration.class)
@SpringBootApplication
public class IngestionApplication {
  public static void main(String[] args) {
    SpringApplication.run(IngestionApplication.class, args);
  }
}
