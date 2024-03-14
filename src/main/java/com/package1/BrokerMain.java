package com.package1;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.client.RestTemplate;

import groovy.util.logging.Slf4j;

@SpringBootApplication
@EnableScheduling
@Slf4j
public class BrokerMain {
    public static void main(String[] args) {
        try {
            SpringApplication.run(BrokerMain.class, args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

}