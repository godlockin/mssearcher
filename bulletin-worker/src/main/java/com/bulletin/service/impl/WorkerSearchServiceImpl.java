package com.bulletin.service.impl;

import com.common.SysConfigUtil;
import com.github.benmanes.caffeine.cache.Cache;
import com.model.DocItem;
import com.service.WorkerQuServiceInterface;
import com.service.WorkerSearchServiceInterface;
import com.service.worker.AbstractWorkerSearchService;
import com.service.worker.operators.WorkerSearchOperator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class WorkerSearchServiceImpl extends AbstractWorkerSearchService implements WorkerSearchServiceInterface {

    @Value("${SEARCH_INSTANCE_KEY:BULLETIN_SEARCH_WORKER}")
    private String INSTANCE_KEY;

    @Value("${SEARCH_WORKER_SEARCH_HANDLER_KEY:BULLETIN_SEARCH}")
    private String HANDLER_KEY;

    @Value("${SEARCH_WORKER_SEARCH_TIMEOUT:2000}")
    private int TIMEOUT;

    @Autowired
    private Map<String, WorkerSearchOperator> operators;

    @Autowired
    private WorkerQuServiceInterface quService;

    private Cache<String, List<DocItem>> localCache;
    private Cache<String, List<DocItem>> redisCache;

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

    @Override
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