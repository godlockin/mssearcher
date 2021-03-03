package com.common;

import com.common.datasource.RedisClient;
import com.model.config.RedisConfigBean;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class RedisClientUtil {
    private static final ConcurrentHashMap<String, RedisClient> cache = new ConcurrentHashMap<>();

    public static RedisClient getClient(String key, RedisConfigBean initConfig) {
        if (StringUtils.isBlank(key)) {
            log.warn("Empty key found");
            return null;
        }

        if (!cache.containsKey(key)) {
            synchronized (RedisClientUtil.class) {
                if (!cache.containsKey(key)) {
                    cache.putIfAbsent(key, new RedisClient(initConfig));
                }
            }
        }
        return cache.get(key);
    }

    public static RedisClient initClient(String key, RedisConfigBean initConfig) {
        if (StringUtils.isBlank(key)) {
            log.warn("Empty key found");
            return null;
        }

        RedisClient client = new RedisClient(initConfig);

        cache.putIfAbsent(key, client);

        return client;
    }

    @PreDestroy
    void destroy() {
        cache.values().forEach(RedisClient::destroy);
    }
}
