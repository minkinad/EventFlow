package io.github.minkin.eventflow.delivery;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@org.springframework.context.annotation.Import(
    io.github.minkin.eventflow.security.ApiSecurityConfiguration.class)
@SpringBootApplication
public class DeliveryApplication {
  public static void main(String[] args) {
    SpringApplication.run(DeliveryApplication.class, args);
  }
}
