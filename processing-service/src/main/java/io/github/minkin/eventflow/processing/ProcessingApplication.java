package io.github.minkin.eventflow.processing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class ProcessingApplication {
    public static void main(String[] args) {
        SpringApplication.run(ProcessingApplication.class, args);
    }
}
