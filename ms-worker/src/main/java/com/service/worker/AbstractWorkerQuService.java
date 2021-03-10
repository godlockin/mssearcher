package com.service.worker;

import com.alibaba.fastjson.JSONObject;
import com.common.OperatorConfigCache;
import com.common.SysConfigUtil;
import com.common.utils.ExtraCollectionUtils;
import com.common.utils.GuidService;
import com.github.benmanes.caffeine.cache.Cache;
import com.model.input.QueryConfig;
import com.model.input.WorkerCoreQuery;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

@Slf4j
@Service
public abstract class AbstractWorkerQuService extends AbstractWorkerCacheAbleService<WorkerCoreQuery, QueryConfig> {

    @Value("${SEARCH_WORKER_QU_HANDLER_KEY:GLOBAL_SEARCH_WORKER_QU}")
    private String HANDLER_KEY;
    @Value("${SEARCH_WORKER_QU_OPERATOR_ACTIVE:true}")
    protected boolean OPERATOR_ACTIVE;

    @Value("${SEARCH_WORKER_QU_CONFIDENCE:1.0}")
    private double CONFIDENCE;

    @Value("${SEARCH_WORKER_QU_CACHE_TYPE:REDIS}")
    private String CACHE_TYPE;
    @Value("${SEARCH_WORKER_QU_CACHE_MAX_SIZE:1000}")
    private Integer CACHE_MAX_SIZE;
    @Value("${SEARCH_WORKER_QU_CACHE_CAPACITY:100}")
    private Integer CACHE_CAPACITY;
    @Value("${SEARCH_WORKER_QU_CACHE_EXPIRE_TIME:60}")
    private Integer CACHE_EXPIRE_TIME;

    @Value("${SEARCH_WORKER_QU_REDIS_HOST:redis}")
    private String REDIS_HOST;
    @Value("${SEARCH_WORKER_QU_REDIS_PORT:6379}")
    private Integer REDIS_PORT;
    @Value("${SEARCH_WORKER_QU_REDIS_PASSWORD:9a46259f1b75feaa}")
    private String REDIS_PASSWORD;
    @Value("${SEARCH_WORKER_QU_REDIS_DATABASE:0}")
    private Integer REDIS_DATABASE;

    @Value("${SEARCH_WORKER_QU_REDIS_PROTOCOL_TIMEOUT:2000}")
    private Integer REDIS_PROTOCOL_TIMEOUT;
    @Value("${SEARCH_WORKER_QU_REDIS_MAX_IDLE:3}")
    private Integer REDIS_MAX_IDLE;
    @Value("${SEARCH_WORKER_QU_REDIS_MAX_TOTAL:1000}")
    private Integer REDIS_MAX_TOTAL;
    @Value("${SEARCH_WORKER_QU_REDIS_MAX_WAIT_MILLIS:10}")
    private Integer REDIS_MAX_WAIT_MILLIS;

    @Value("${SEARCH_WORKER_QU_REDIS_MIN_EVICTABLE_IDLE_TIME_MILLIS:1000}")
    private Long REDIS_MIN_EVICTABLE_IDLE_TIME_MILLIS;
    @Value("${SEARCH_WORKER_QU_REDIS_NUM_TESTS_PER_EVICTION_RUN:3}")
    private Integer REDIS_NUM_TESTS_PER_EVICTION_RUN;
    @Value("${SEARCH_WORKER_QU_REDIS_TIME_BETWEEN_EVICTION_RUNS_MILLIS:3}")
    private Integer REDIS_TIME_BETWEEN_EVICTION_RUNS_MILLIS;
    @Value("${SEARCH_WORKER_QU_REDIS_TEST_ON_BORROW:true}")
    private Boolean REDIS_TEST_ON_BORROW;
    @Value("${SEARCH_WORKER_QU_REDIS_TEST_WHILE_IDLE:true}")
    private Boolean REDIS_TEST_WHILE_IDLE;

    private Cache<String, QueryConfig> localCache;
    private Cache<String, QueryConfig> redisCache;

    @Override
    protected Cache<String, QueryConfig> localCache() {
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
    protected Cache<String, QueryConfig> redisCache() {
        if (ObjectUtils.isEmpty(this.redisCache)) {
            synchronized (this) {
                if (ObjectUtils.isEmpty(this.redisCache)) {
                    this.redisCache = buildRedisCache();
                }
            }
        }
        return this.redisCache;
    }

    public String cacheKey(WorkerCoreQuery query) {
        String hash = GuidService.getXXHash(query.getQuery());
        return String.format(cacheKeyPattern()
                , SysConfigUtil.getAsString("ServiceInstance", "INSTANCE_KEY", "GLOBAL")
                , handlerKey()
                , hash
        );
    }

    public QueryConfig defaultResult() {
        return QueryConfig.builder()
                .globalRate(confidence())
                .rateMapping(OperatorConfigCache.loadAll())
                .build();
    }

    @Override
    protected Map<String, Object> doBuildParam(WorkerCoreQuery coreQuery) {
        return CollectionUtils.newHashMap(0);
    }

    @Override
    protected Map<String, Object> doDataQuery(WorkerCoreQuery coreQuery, Map<String, Object> param) {
        return CollectionUtils.newHashMap(0);
    }

    @Override
    protected QueryConfig doParseResult(WorkerCoreQuery coreQuery, Map<String, Object> queryResultMap) {
        return defaultResult();
    }

    @Override
    protected Function<String, QueryConfig> redisCacheLoadConverter() {
        return string -> JSONObject.parseObject(string, QueryConfig.class);
    }

    @Override
    protected Predicate<QueryConfig> cacheDataPredicate() {
        return config -> super.cacheDataPredicate().test(config)
                && ExtraCollectionUtils.isNotEmpty(config.getRateMapping());
    }

    @Override
    protected Function<QueryConfig, String> redisCacheSaveConverter() {
        return JSONObject::toJSONString;
    }

    @Override
    public Map<String, Object> findConfig() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("HANDLER_KEY", HANDLER_KEY);
        config.put("OPERATOR_ACTIVE", OPERATOR_ACTIVE);
        config.put("CONFIDENCE", CONFIDENCE);

        config.put("CACHE_TYPE", CACHE_TYPE);
        config.put("CACHE_MAX_SIZE", CACHE_MAX_SIZE);
        config.put("CACHE_CAPACITY", CACHE_CAPACITY);
        config.put("CACHE_EXPIRE_TIME", CACHE_EXPIRE_TIME);

        config.put("REDIS_HOST", REDIS_HOST);
        config.put("REDIS_PORT", REDIS_PORT);
        config.put("REDIS_PASSWORD", REDIS_PASSWORD);
        config.put("REDIS_DATABASE", REDIS_DATABASE);

        config.put("REDIS_PROTOCOL_TIMEOUT", REDIS_PROTOCOL_TIMEOUT);
        config.put("REDIS_MAX_IDLE", REDIS_MAX_IDLE);
        config.put("REDIS_MAX_TOTAL", REDIS_MAX_TOTAL);
        config.put("REDIS_MAX_WAIT_MILLIS", REDIS_MAX_WAIT_MILLIS);
        config.put("REDIS_MIN_EVICTABLE_IDLE_TIME_MILLIS", REDIS_MIN_EVICTABLE_IDLE_TIME_MILLIS);
        config.put("REDIS_NUM_TESTS_PER_EVICTION_RUN", REDIS_NUM_TESTS_PER_EVICTION_RUN);
        config.put("REDIS_TIME_BETWEEN_EVICTION_RUNS_MILLIS", REDIS_TIME_BETWEEN_EVICTION_RUNS_MILLIS);
        config.put("REDIS_TEST_ON_BORROW", REDIS_TEST_ON_BORROW);
        config.put("REDIS_TEST_WHILE_IDLE", REDIS_TEST_WHILE_IDLE);
        return config;
    }
}
