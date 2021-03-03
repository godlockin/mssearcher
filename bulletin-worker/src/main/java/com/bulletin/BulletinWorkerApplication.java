package com.bulletin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com")
public class BulletinWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(BulletinWorkerApplication.class, args);
    }

}
