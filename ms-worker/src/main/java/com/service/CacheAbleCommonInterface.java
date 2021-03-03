package com.service;

import com.common.SysConfigUtil;
import com.github.benmanes.caffeine.cache.Cache;

public interface CacheAbleCommonInterface<T, R> extends CommonInterface<T, R> {
    default String cacheType() { return SysConfigUtil.getAsString(operatorName(),"CACHE_TYPE", "NONE"); }

    default Integer cacheMaxSize() { return SysConfigUtil.getAsInteger(operatorName(),"CACHE_MAX_SIZE", 1000); }

    default Integer cacheCapacity() { return SysConfigUtil.getAsInteger(operatorName(),"CACHE_CAPACITY", 100); }

    default Integer cacheExpireTime() { return SysConfigUtil.getAsInteger(operatorName(),"CACHE_EXPIRE_TIME", 60); }

    default String cacheKeyPattern() { return SysConfigUtil.getAsString(operatorName(),"CACHE_KEY_PATTERN", "%s:%s:%s"); }

    <T> Cache<String, T> getCache();

    String cacheKey(T t);

    R fetchFunc(T param);
}