package com.news.service.impl;

import com.service.WorkerQuServiceInterface;
import com.service.worker.AbstractWorkerQuService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Map;

@Slf4j
@Service
public class WorkerQuServiceImpl extends AbstractWorkerQuService implements WorkerQuServiceInterface {

    @Value("${SEARCH_WORKER_QU_HANDLER_KEY:NEWS_SEARCH_QU}")
    private String HANDLER_KEY;

    @Value("${SEARCH_WORKER_QU_TIMEOUT:2000}")
    private int TIMEOUT;

    @Override
    @PostConstruct
    protected void init() {
        super.init();
    }

    @Override
    public Map<String, Object> findConfig() {
        Map<String, Object> config = super.findConfig();
        config.put("HANDLER_KEY", HANDLER_KEY);
        config.put("TIMEOUT", TIMEOUT);
        return config;
    }
}