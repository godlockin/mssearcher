package com.common;

import com.model.config.ESConfigBean;
import com.common.datasource.ESClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class ESClientUtil {
    private static final ConcurrentHashMap<String, ESClient> cache = new ConcurrentHashMap<>();

    public static void set(String key, ESClient client) {
        cache.put(key, client);
    }

    public static ESClient getClient(String key, ESConfigBean initConfig) {
        if (StringUtils.isBlank(key)) {
            log.warn("Empty key found");
            return null;
        }

        if (!cache.containsKey(key)) {
            synchronized (ESClientUtil.class) {
                if (!cache.containsKey(key)) {
                    cache.putIfAbsent(key, new ESClient(initConfig));
                }
            }
        }
        return cache.get(key);
    }

    public static ESClient initClient(String key, ESConfigBean initConfig) {
        if (StringUtils.isBlank(key)) {
            log.warn("Empty key found");
            return null;
        }

        ESClient client = new ESClient(initConfig);

        cache.putIfAbsent(key, client);

        return client;
    }

    @PreDestroy
    void destroy() {
        cache.values().forEach(ESClient::destroy);
    }
}
