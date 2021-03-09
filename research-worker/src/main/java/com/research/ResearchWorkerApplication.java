package com.research;

import com.ctrip.framework.apollo.spring.annotation.EnableApolloConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableApolloConfig
@SpringBootApplication(scanBasePackages = "com")
public class ResearchWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ResearchWorkerApplication.class, args);
    }
}
