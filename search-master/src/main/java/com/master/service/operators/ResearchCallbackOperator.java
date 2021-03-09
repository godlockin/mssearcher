package com.master.service.operators;

import com.github.benmanes.caffeine.cache.Cache;
import com.model.DocItem;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class ResearchCallbackOperator extends WorkerCallbackOperator {

    @Value("${RESEARCH_CALLBACK_HANDLER_KEY:RESEARCH_CALLBACK}")
    private String HANDLER_KEY;
    @Value("${RESEARCH_CALLBACK_OPERATOR_ACTIVE:true}")
    private boolean OPERATOR_ACTIVE;

    @Value("${RESEARCH_CALLBACK_DATA_TYPE:research}")
    private String DATA_TYPE;

    @Value("${RESEARCH_CALLBACK_CONFIDENCE:1.0}")
    private double CONFIDENCE;

    @Value("${RESEARCH_CALLBACK_OPERATOR_TIMEOUT:2000}")
    private int TIMEOUT;

    @Value("${RESEARCH_CALLBACK_REMOTE_URL:http://research-search-worker:8080/v1/api/search}")
    private String REMOTE_URL;
    @Value("${RESEARCH_CALLBACK_RESULT_DATA_KEY:data}")
    private String RESULT_DATA_KEY;
    @Value("${RESEARCH_CALLBACK_SS_CODE:1}")
    private String SS_CODE;
    @Value("${RESEARCH_CALLBACK_SS_CODE_KEY:code}")
    private String SS_CODE_KEY;

    @Value("${RESEARCH_CALLBACK_CACHE_TYPE:REDIS}")
    protected String CACHE_TYPE;
    @Value("${RESEARCH_CALLBACK_CACHE_MAX_SIZE:1000}")
    protected Integer CACHE_MAX_SIZE;
    @Value("${RESEARCH_CALLBACK_CACHE_CAPACITY:100}")
    protected Integer CACHE_CAPACITY;
    @Value("${RESEARCH_CALLBACK_CACHE_EXPIRE_TIME:60}")
    protected Integer CACHE_EXPIRE_TIME;

    @Value("${RESEARCH_CALLBACK_REDIS_HOST:redis}")
    protected String REDIS_HOST;
    @Value("${RESEARCH_CALLBACK_REDIS_PORT:6379}")
    protected Integer REDIS_PORT;
    @Value("${RESEARCH_CALLBACK_REDIS_PASSWORD:9a46259f1b75feaa}")
    protected String REDIS_PASSWORD;
    @Value("${RESEARCH_CALLBACK_REDIS_DATABASE:0}")
    protected Integer REDIS_DATABASE;

    @Value("${RESEARCH_CALLBACK_REDIS_PROTOCOL_TIMEOUT:2000}")
    protected Integer REDIS_PROTOCOL_TIMEOUT;
    @Value("${RESEARCH_CALLBACK_REDIS_MAX_IDLE:3}")
    protected Integer REDIS_MAX_IDLE;
    @Value("${RESEARCH_CALLBACK_REDIS_MAX_TOTAL:1000}")
    protected Integer REDIS_MAX_TOTAL;
    @Value("${RESEARCH_CALLBACK_REDIS_MAX_WAIT_MILLIS:10}")
    protected Integer REDIS_MAX_WAIT_MILLIS;

    @Value("${RESEARCH_CALLBACK_REDIS_MIN_EVICTABLE_IDLE_TIME_MILLIS:1000}")
    protected Long REDIS_MIN_EVICTABLE_IDLE_TIME_MILLIS;
    @Value("${RESEARCH_CALLBACK_REDIS_NUM_TESTS_PER_EVICTION_RUN:3}")
    protected Integer REDIS_NUM_TESTS_PER_EVICTION_RUN;
    @Value("${RESEARCH_CALLBACK_REDIS_TIME_BETWEEN_EVICTION_RUNS_MILLIS:3}")
    protected Integer REDIS_TIME_BETWEEN_EVICTION_RUNS_MILLIS;
    @Value("${RESEARCH_CALLBACK_REDIS_TEST_ON_BORROW:true}")
    protected Boolean REDIS_TEST_ON_BORROW;
    @Value("${RESEARCH_CALLBACK_REDIS_TEST_WHILE_IDLE:true}")
    protected Boolean REDIS_TEST_WHILE_IDLE;

    private Cache<String, List<DocItem>> localCache;
    private Cache<String, List<DocItem>> redisCache;

    @PostConstruct
    protected void init() {
        super.init();
    }

    @Override
    protected Cache<String, List<DocItem>> localCache() {
        if (ObjectUtils.isEmpty(this.localCache)) {
            synchronized (this) {
                if (ObjectUtils.isEmpty(this.localCache)) {
                    this.localCache = buildLocalCache();
                }
            }
        }
        return this.localCache;
    }

    @Override
    protected Cache<String, List<DocItem>> redisCache() {
        if (ObjectUtils.isEmpty(this.redisCache)) {
            synchronized (this) {
                if (ObjectUtils.isEmpty(this.redisCache)) {
                    this.redisCache = buildRedisCache();
                }
            }
        }
        return this.redisCache;
    }

    @Override
    public Map<String, Object> findConfig() {
        Map<String, Object> config = super.findConfig();
        config.put("HANDLER_KEY", HANDLER_KEY);
        config.put("OPERATOR_ACTIVE", OPERATOR_ACTIVE);

        config.put("TIMEOUT", TIMEOUT);
        config.put("DATA_TYPE", DATA_TYPE);
        config.put("CONFIDENCE", CONFIDENCE);

        config.put("REMOTE_URL", REMOTE_URL);
        config.put("SS_CODE", SS_CODE);
        config.put("SS_CODE_KEY", SS_CODE_KEY);
        config.put("RESULT_DATA_KEY", RESULT_DATA_KEY);

        config.put("CACHE_TYPE", CACHE_TYPE);
        config.put("CACHE_MAX_SIZE", CACHE_MAX_SIZE);
        config.put("CACHE_CAPACITY", CACHE_CAPACITY);
        config.put("CACHE_EXPIRE_TIME", CACHE_EXPIRE_TIME);

        config.put("REDIS_HOST", REDIS_HOST);
        config.put("REDIS_PORT", REDIS_PORT);
        config.put("REDIS_PASSWORD", REDIS_PASSWORD);
        config.put("REDIS_DATABASE", REDIS_DATABASE);

        config.put("REDIS_MAX_IDLE", REDIS_MAX_IDLE);
        config.put("REDIS_MAX_TOTAL", REDIS_MAX_TOTAL);
        config.put("REDIS_TEST_ON_BORROW", REDIS_TEST_ON_BORROW);
        config.put("REDIS_TEST_WHILE_IDLE", REDIS_TEST_WHILE_IDLE);
        config.put("REDIS_MAX_WAIT_MILLIS", REDIS_MAX_WAIT_MILLIS);
        config.put("REDIS_PROTOCOL_TIMEOUT", REDIS_PROTOCOL_TIMEOUT);
        config.put("REDIS_NUM_TESTS_PER_EVICTION_RUN", REDIS_NUM_TESTS_PER_EVICTION_RUN);
        config.put("REDIS_MIN_EVICTABLE_IDLE_TIME_MILLIS", REDIS_MIN_EVICTABLE_IDLE_TIME_MILLIS);
        config.put("REDIS_TIME_BETWEEN_EVICTION_RUNS_MILLIS", REDIS_TIME_BETWEEN_EVICTION_RUNS_MILLIS);
        return config;
    }
}
