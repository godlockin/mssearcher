package com.news;

import com.ctrip.framework.apollo.spring.annotation.EnableApolloConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableApolloConfig
@SpringBootApplication(scanBasePackages = "com")
public class NewsWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(NewsWorkerApplication.class, args);
    }

}
