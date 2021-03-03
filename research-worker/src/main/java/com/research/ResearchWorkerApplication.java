package com.research;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com")
public class ResearchWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ResearchWorkerApplication.class, args);
    }

}
