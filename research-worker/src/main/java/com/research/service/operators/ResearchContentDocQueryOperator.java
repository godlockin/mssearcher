package com.research.service.operators;

import com.common.SysConfigUtil;
import com.common.datasource.ESClient;
import com.github.benmanes.caffeine.cache.Cache;
import com.model.DocItem;
import com.service.worker.operators.WorkerDocQueryOperator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class ResearchContentDocQueryOperator extends WorkerDocQueryOperator {

    @Value("${RESEARCH_CONTENT_DOC_QUERY_HANDLER_KEY:RESEARCH_CONTENT_DOC_QUERY}")
    private String HANDLER_KEY;
    @Value("${RESEARCH_CONTENT_DOC_QUERY_OPERATOR_ACTIVE:true}")
    private boolean OPERATOR_ACTIVE;

    @Value("${RESEARCH_CONTENT_DOC_QUERY_CONFIDENCE:0.2}")
    private double CONFIDENCE;
    @Value("${RESEARCH_CONTENT_DOC_QUERY_DATA_TYPE:content}")
    private String DATA_TYPE;

    @Value("${RESEARCH_CONTENT_DOC_QUERY_OPERATOR_TIMEOUT:2000}")
    private int TIMEOUT;

    @Value("${RESEARCH_CONTENT_DOC_QUERY_CACHE_TYPE:REDIS}")
    private String CACHE_TYPE;

    @Value("${RESEARCH_CONTENT_DOC_QUERY_ES_ADDRESS:localhost:9200}")
    private String ES_ADDRESS;
    @Value("${RESEARCH_CONTENT_DOC_QUERY_ES_USERNAME:}")
    private String ES_USERNAME;
    @Value("${RESEARCH_CONTENT_DOC_QUERY_ES_PASSWORD:}")
    private String ES_PASSWORD;
    @Value("${RESEARCH_CONTENT_DOC_QUERY_ES_INDEX:}")
    private String ES_INDEX;

    @Value("${RESEARCH_CONTENT_DOC_QUERY_DECAY_TYPE:SIGMOID}")
    private String DECAY_TYPE;
    @Value("${RESEARCH_CONTENT_DOC_QUERY_TIME_DECAY:0.8}")
    private double TIME_DECAY;

    private ESClient esClient;
    private Cache<String, List<DocItem>> localCache;
    private Cache<String, List<DocItem>> redisCache;

    @Override
    @PostConstruct
    protected void init() {
        super.init();
        esClient = buildESClient();
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
    protected ESClient esClient() {
        return this.esClient;
    }

    @Override
    protected String decayType() {
        return SysConfigUtil.getAsString("DECAY_TYPE", DECAY_TYPE);
    }

    @Override
    protected double workerTimeDecayRate() {
        return SysConfigUtil.getAsDouble("TIME_DECAY", TIME_DECAY);
    }

    @Override
    protected String esIndex() {
        return SysConfigUtil.getAsString("ES_INDEX", ES_INDEX);
    }

    @Override
    public Map<String, Object> findConfig() {
        Map<String, Object> config = super.findConfig();
        config.put("HANDLER_KEY", HANDLER_KEY);
        config.put("OPERATOR_ACTIVE", OPERATOR_ACTIVE);

        config.put("TIMEOUT", TIMEOUT);
        config.put("DATA_TYPE", DATA_TYPE);
        config.put("CONFIDENCE", CONFIDENCE);
        config.put("CACHE_TYPE", CACHE_TYPE);

        config.put("ES_ADDRESS", ES_ADDRESS);
        config.put("ES_USERNAME", ES_USERNAME);
        config.put("ES_PASSWORD", ES_PASSWORD);
        config.put("ES_INDEX", ES_INDEX);

        config.put("DECAY_TYPE", DECAY_TYPE);
        config.put("TIME_DECAY", TIME_DECAY);
        return config;
    }
}
