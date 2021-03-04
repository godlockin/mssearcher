package com.news.service.impl;

import com.common.utils.ExtraCollectionUtils;
import com.github.benmanes.caffeine.cache.Cache;
import com.common.SysConfigUtil;
import com.model.DocItem;
import com.model.SortItem;
import com.service.WorkerSearchServiceInterface;
import com.service.WorkerQuServiceInterface;
import com.service.worker.AbstractWorkerSearchService;
import com.service.worker.operators.WorkerSearchOperator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@Slf4j
@Service
public class WorkerSearchServiceImpl extends AbstractWorkerSearchService implements WorkerSearchServiceInterface {

    @Value("${SEARCH_INSTANCE_KEY:NEWS_SEARCH_WORKER}")
    private String INSTANCE_KEY;

    @Value("${SEARCH_WORKER_SEARCH_HANDLER_KEY:NEWS_SEARCH}")
    private String HANDLER_KEY;

    @Value("${SEARCH_WORKER_SEARCH_TIMEOUT:2000}")
    private int TIMEOUT;

    private Cache<String, List<DocItem>> localCache;
    private Cache<String, List<DocItem>> redisCache;

    @Autowired
    private Map<String, WorkerSearchOperator> operators;

    @Autowired
    private WorkerQuServiceInterface quService;

    protected ConcurrentMap<String, List<DocItem>> doBuildDocGroupsMap(Map<String, Object> queryResultMap) {
        Set<String> distinct = ConcurrentHashMap.newKeySet();
        return queryResultMap
                .entrySet().parallelStream()
                .filter(e -> e.getValue() instanceof List)
                .map(e -> (List<DocItem>) e.getValue())
                .filter(ExtraCollectionUtils::isNotEmpty)
                .flatMap(Collection::stream)
                .filter(docItem -> distinct.add(docItem.getFuncId()))
                .filter(docItem -> distinct.add(docItem.getTitle()))
                .sorted(Comparator.comparing(DocItem::getFinalScore).reversed())
                .collect(Collectors.groupingByConcurrent(DocItem::getBundleKey));
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
    protected Map<String, WorkerSearchOperator> operators() { return this.operators; }

    @Override
    protected WorkerQuServiceInterface quService() { return this.quService; }

    @PostConstruct
    protected void init() {
        super.init();
        SysConfigUtil.set("ServiceInstance", "INSTANCE_KEY", INSTANCE_KEY);
    }

    @Override
    public Map<String, Object> findConfig() {
        Map<String, Object> config = super.findConfig();
        config.put("HANDLER_KEY", HANDLER_KEY);
        config.put("TIMEOUT", TIMEOUT);
        return config;
    }
}