package com.master;

import com.ctrip.framework.apollo.spring.annotation.EnableApolloConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

//@EnableApolloConfig
@SpringBootApplication(scanBasePackages = "com")
public class SearchMasterApplication {

    public static void main(String[] args) {
        SpringApplication.run(SearchMasterApplication.class, args);
    }
}
