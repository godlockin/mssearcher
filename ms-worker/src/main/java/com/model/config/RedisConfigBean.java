package com.model.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RedisConfigBean {
    private String redisHost;
    private Integer redisPort;
    private String redisPassword;
    private Integer redisDatabase;

    private Integer protocolTimeout;
    private Integer maxIdle;
    private Integer maxTotal;
    private Integer maxWaitMS;
    private Long minEvictableIdleTimeMS;
    private Integer numTestPreEvictionRun;
    private Integer timeBetweenEvictionRunsMS;
    private Boolean testOnBorrow;
    private Boolean testWhileIdle;
}
