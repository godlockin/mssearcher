package com.news;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com")
public class NewsWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(NewsWorkerApplication.class, args);
    }

}
