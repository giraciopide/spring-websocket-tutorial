package dev.dimlight.tutorial.spring.ws.mvc.client;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@Configuration
@EnableScheduling
public class SimpleClientMain {

    public static void main(String[] args) {
        SpringApplication.run(SimpleClientMain.class, args);
    }
}
