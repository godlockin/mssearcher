package com.earning;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com")
public class EarningWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(EarningWorkerApplication.class, args);
    }

}
