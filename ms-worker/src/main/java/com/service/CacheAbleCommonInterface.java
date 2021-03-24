package com.service;

import com.common.SysConfigUtil;
import com.github.benmanes.caffeine.cache.Cache;

/**
 * cacheable service base interface for all cacheable services in system
 * @param <T> the input param
 * @param <R> the output data
 */
public interface CacheAbleCommonInterface<T, R> extends CommonInterface<T, R> {

    /**
     * the cache type for this service
     *  - NONE for no cache
     *  - LOCAL for local cache, implemented by caffeine
     *  - REDIS for redis cache, connection info should be config
     *
     * @return cache type
     */
    default String cacheType() { return SysConfigUtil.getAsString(operatorName(),"CACHE_TYPE", "NONE"); }

    /**
     * the max size for this cache
     *
     * @return max size
     */
    default Integer cacheMaxSize() { return SysConfigUtil.getAsInteger(operatorName(),"CACHE_MAX_SIZE", 1000); }

    /**
     * capacity for this cache
     *
     * @return capacity
     */
    default Integer cacheCapacity() { return SysConfigUtil.getAsInteger(operatorName(),"CACHE_CAPACITY", 100); }

    /**
     * cache expire time
     *
     * @return expire time
     */
    default Integer cacheExpireTime() { return SysConfigUtil.getAsInteger(operatorName(),"CACHE_EXPIRE_TIME", 60); }

    /**
     * cache key pattern, e.g. WORKER_NAME:SERVICE_NAME:${hash of query}
     *  will be used as String.format(${key pattern}, ${some key}, ${some hash});
     *
     * @return key pattern
     */
    default String cacheKeyPattern() { return SysConfigUtil.getAsString(operatorName(),"CACHE_KEY_PATTERN", "%s:%s:%s"); }

    /**
     * cache instance
     *
     * @param <T> data type of cached items
     * @return cache instance
     */
    <T> Cache<String, T> getCache();

    /**
     * real cache key for each query
     *
     * @param t query parameter
     * @return real cache key
     */
    String cacheKey(T t);

    /**
     * query function in service
     *
     * @param param query parameter
     * @return query result
     */
    R fetchFunc(T param);
}