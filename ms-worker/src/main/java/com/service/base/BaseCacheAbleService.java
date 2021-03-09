package com.service.base;

import com.alibaba.fastjson.JSONValidator;
import com.common.RedisClientUtil;
import com.common.SysConfigUtil;
import com.common.constants.CacheTypeEnum;
import com.common.constants.Constants.RedisConfig;
import com.common.constants.ResultEnum;
import com.common.datasource.RedisClient;
import com.common.utils.GuidService;
import com.exception.MsWorkerException;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.model.config.RedisConfigBean;
import com.service.CacheAbleCommonInterface;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

@Slf4j
@Service
public abstract class BaseCacheAbleService<T, R> extends BaseService<T, R> implements CacheAbleCommonInterface<T, R> {

    protected volatile boolean cacheEnable = false;
    protected CacheTypeEnum cacheTypeEnum = CacheTypeEnum.NONE;

    @Override
    public R handle(T param) {
        log.debug("{} do handle", handlerKey());

        R r;
        String key = cacheKey(param);
        Cache<String, R> cacheInstance = getCache();
        Function<String, R> function = id -> fetchFunc(param);
        if (ObjectUtils.isEmpty(cacheInstance)) {
            r = function.apply(key);
        } else {
            r = cacheInstance.get(key, function);
        }

        log.debug("{} done handle", handlerKey());
        return r;
    }

    @Override
    public R fetchFunc(T param) {
        log.debug("{} do fetchFunc", handlerKey());
        R r = super.fetchFunc(param);
        log.debug("{} done fetchFunc", handlerKey());
        return r;
    }

    public <T> Cache<String, T> getCache() {
        cacheTypeEnum = CacheTypeEnum.parse(cacheType());
        switch (cacheTypeEnum) {
            case REDIS:
                return localCache();

            case LOCAL:
                return redisCache();

            case NONE:
            default:
                return null;
        }
    }

    protected abstract <T> Cache<String, T> redisCache();

    protected abstract <T> Cache<String, T> localCache();

    protected abstract Map<String, Object> doBuildParam(T t);

    protected abstract R doParseResult(T t, Map<String, Object> queryResultMap);

    protected void cacheInit() {
        cacheTypeEnum = CacheTypeEnum.parse(cacheType());
        switch (cacheTypeEnum) {
            case REDIS:
                redisCache();
                cacheEnable = true;
                break;

            case LOCAL:
                localCache();
                cacheEnable = true;
                break;

            case NONE:
            default:
                cacheEnable = false;
                break;
        }
    }

    public String cacheKey(T t) {
        String cacheKey = String.format(cacheKeyPattern()
                , SysConfigUtil.getAsString("ServiceInstance", "INSTANCE_KEY", "GLOBAL")
                , handlerKey()
                , GuidService.getXXHash(String.valueOf(t))
        );
        log.info("{} cacheKey:[{}]", operatorName(), cacheKey);
        return cacheKey;
    }

    protected <T> Cache<String, T> buildLocalCache() {
        return Caffeine.newBuilder()
                .expireAfterAccess(Long.valueOf(cacheExpireTime()), TimeUnit.SECONDS)
                .initialCapacity(cacheCapacity())
                .maximumSize(cacheMaxSize())
                .build();
    }

    protected RedisConfigBean buildRedisConfigBean() {
        return RedisConfigBean.builder()
                .redisHost(SysConfigUtil.getAsString(operatorName(), "REDIS_HOST", RedisConfig.DEFAULT_REDIS_HOST))
                .redisPort(SysConfigUtil.getAsInteger(operatorName(), "REDIS_PORT", RedisConfig.DEFAULT_REDIS_PORT))
                .redisPassword(SysConfigUtil.getAsString(operatorName(), "REDIS_PASSWORD", RedisConfig.DEFAULT_REDIS_PASSWORD))
                .redisDatabase(SysConfigUtil.getAsInteger(operatorName(), "REDIS_DATABASE", RedisConfig.DEFAULT_REDIS_DATABASE))
                .protocolTimeout(SysConfigUtil.getAsInteger(operatorName(), "REDIS_PROTOCOL_TIMEOUT", RedisConfig.DEFAULT_REDIS_PROTOCOL_TIMEOUT))
                .maxIdle(SysConfigUtil.getAsInteger(operatorName(), "REDIS_MAX_IDLE", RedisConfig.DEFAULT_REDIS_MAX_IDLE))
                .maxTotal(SysConfigUtil.getAsInteger(operatorName(), "REDIS_MAX_TOTAL", RedisConfig.DEFAULT_REDIS_MAX_TOTAL))
                .maxWaitMS(SysConfigUtil.getAsInteger(operatorName(), "REDIS_MAX_WAIT_MILLIS", RedisConfig.DEFAULT_REDIS_MAX_WAIT_MILLIS))
                .minEvictableIdleTimeMS(SysConfigUtil.getAsLong(operatorName(), "REDIS_MIN_EVICTABLE_IDLE_TIME_MILLIS", RedisConfig.DEFAULT_REDIS_MIN_EVICTABLE_IDLE_TIME_MILLIS))
                .numTestPreEvictionRun(SysConfigUtil.getAsInteger(operatorName(), "REDIS_NUM_TESTS_PER_EVICTION_RUN", RedisConfig.DEFAULT_REDIS_NUM_TESTS_PER_EVICTION_RUN))
                .timeBetweenEvictionRunsMS(SysConfigUtil.getAsInteger(operatorName(), "REDIS_TIME_BETWEEN_EVICTION_RUNS_MILLIS", RedisConfig.DEFAULT_REDIS_TIME_BETWEEN_EVICTION_RUNS_MILLIS))
                .testOnBorrow(SysConfigUtil.getAsBoolean(operatorName(), "REDIS_TEST_ON_BORROW", RedisConfig.DEFAULT_REDIS_TEST_ON_BORROW))
                .testWhileIdle(SysConfigUtil.getAsBoolean(operatorName(), "REDIS_TEST_WHILE_IDLE", RedisConfig.DEFAULT_REDIS_TEST_ON_BORROW))
                .build();
    }

    protected Predicate<String> redisCachePredicate() {
        return content -> StringUtils.isNotBlank(content) && JSONValidator.from(content).validate();
    }

    protected abstract Function<String, R> redisCacheLoadConverter();

    protected <R> Predicate<R> cacheDataPredicate() {
        return r -> !ObjectUtils.isEmpty(r);
    }

    protected abstract <R> Function<R, String> redisCacheSaveConverter();

    protected <R> Cache<String, R> buildRedisCache() {
        RedisClient redisClient = RedisClientUtil.initClient(handlerKey(), buildRedisConfigBean());
        return new Cache<String, R>() {

            private volatile CacheStats stats = CacheStats.empty();

            @Nullable
            @Override
            public R getIfPresent(@NonNull Object key) {

                R r = (R) defaultResult();
                String kStr = key.toString().trim();
                if (!cacheEnable || ObjectUtils.isEmpty(redisClient) || StringUtils.isBlank(kStr)) {
                    return r;
                }

                long start = System.nanoTime();
                String content = redisClient.get(kStr);
                if (redisCachePredicate().test(content)) {
                    r = (R) redisCacheLoadConverter().apply(content);

                    boolean loadSs = cacheDataPredicate().test(r);
                    CacheStats other = new CacheStats(
                            loadSs ? 1 : 0
                            , loadSs ? 0 : 1
                            , loadSs ? 1 : 0
                            , loadSs ? 0 : 1
                            , (System.nanoTime() - start) / 1_000_000
                            , 0
                            , 0
                    );
                    stats.plus(other);
                }

                return r;
            }

            @Nullable
            @Override
            public R get(@NonNull String key, @NonNull Function<? super String, ? extends R> mappingFunction) {
                R r = getIfPresent(key);
                if (cacheDataPredicate().test(r)) {
                    return r;
                }

                CacheStats other;
                long start = System.nanoTime();
                r = mappingFunction.apply(key);
                boolean loadSs = cacheDataPredicate().test(r);

                if (loadSs) {
                    put(key, r);
                    other = new CacheStats(
                            1
                            , 0
                            , 1
                            , 0
                            , (System.nanoTime() - start) / 1_000_000
                            , 0
                            , 0
                    );
                } else {
                    r = (R) defaultResult();
                    other = new CacheStats(
                            0
                            , 1
                            , 0
                            , 1
                            , (System.nanoTime() - start) / 1_000_000
                            , 0
                            , 0
                    );
                }

                stats.plus(other);
                return r;
            }

            @Override
            public @NonNull Map<String, R> getAllPresent(@NonNull Iterable<?> keys) {
                Map<String, R> map = new HashMap<>();
                keys.forEach(k -> map.put(String.valueOf(k), getIfPresent(k)));
                return map;
            }

            @Override
            public void put(@NonNull String key, @NonNull R value) {
                if (!cacheDataPredicate().test(value)) {
                    return;
                }

                String content = redisCacheSaveConverter().apply(value);
                if (redisCachePredicate().test(content)) {
                    redisClient.set(key, content, cacheExpireTime().longValue());
                }
            }

            @Override
            public void putAll(@NonNull Map<? extends String, ? extends R> map) {
                map.entrySet().parallelStream().forEach(e -> put(e.getKey(), e.getValue()));
            }

            @Override
            public void invalidate(@NonNull Object key) {
                if (ObjectUtils.isEmpty(key) || StringUtils.isBlank(String.valueOf(key))) {
                    throw new MsWorkerException(ResultEnum.PARAMETER_CHECK);
                }
            }

            @Override
            public void invalidateAll(@NonNull Iterable<?> keys) {
                keys.forEach(this::invalidate);
            }

            @Override
            public void invalidateAll() {
            }

            @Override
            public @NonNegative long estimatedSize() {
                return cacheMaxSize();
            }

            @Override
            public @NonNull CacheStats stats() {
                return stats;
            }

            @Override
            public @NonNull ConcurrentMap<String, R> asMap() {
                throw new MsWorkerException(ResultEnum.ILLEGAL_METHOD);
            }

            @Override
            public void cleanUp() {
                throw new MsWorkerException(ResultEnum.ILLEGAL_METHOD);
            }

            @Override
            public @NonNull Policy<String, R> policy() {
                return new Policy<String, R>() {
                    @Override
                    public boolean isRecordingStats() {
                        return false;
                    }

                    @Override
                    public @NonNull Optional<Eviction<String, R>> eviction() {
                        return Optional.empty();
                    }

                    @Override
                    public @NonNull Optional<Expiration<String, R>> expireAfterAccess() {
                        return Optional.empty();
                    }

                    @Override
                    public @NonNull Optional<Expiration<String, R>> expireAfterWrite() {
                        return Optional.empty();
                    }

                    @Override
                    public @NonNull Optional<Expiration<String, R>> refreshAfterWrite() {
                        return Optional.empty();
                    }
                };
            }
        };
    }

    protected void init() {
        super.init();
        cacheInit();
    }
}