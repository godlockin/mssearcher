package com.news.service.operators;

import com.common.SysConfigUtil;
import com.common.constants.Constants;
import com.common.datasource.ESClient;
import com.github.benmanes.caffeine.cache.Cache;
import com.model.DocItem;
import com.model.input.QueryRequest;
import com.model.input.WorkerCoreQuery;
import com.news.common.NewsUtils;
import com.service.worker.operators.WorkerDocGetOperator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

@Slf4j
@Service
public class NewsDocGetOperator extends WorkerDocGetOperator {

    @Value("${NEWS_DOC_GET_HANDLER_KEY:NEWS_DOC_GET}")
    private String HANDLER_KEY;
    @Value("${NEWS_DOC_GET_OPERATOR_ACTIVE:true}")
    private boolean OPERATOR_ACTIVE;

    @Value("${NEWS_DOC_GET_DATA_TYPE:doc}")
    private String DATA_TYPE;

    @Value("${NEWS_DOC_GET_OPERATOR_TIMEOUT:2000}")
    private int TIMEOUT;

    @Value("${NEWS_DOC_GET_CACHE_TYPE:REDIS}")
    protected String CACHE_TYPE;

    @Value("${NEWS_DOC_GET_ES_ADDRESS:localhost:9200}")
    protected String ES_ADDRESS;
    @Value("${NEWS_DOC_GET_ES_USERNAME:}")
    protected String ES_USERNAME;
    @Value("${NEWS_DOC_GET_ES_PASSWORD:}")
    protected String ES_PASSWORD;
    @Value("${NEWS_DOC_GET_ES_INDEX:}")
    protected String ES_INDEX;

    @Value("${NEWS_DOC_GET_DECAY_TYPE:HYPERBOLIC}")
    private String DECAY_TYPE;
    @Value("${NEWS_DOC_GET_TIME_DECAY:0.4}")
    private double TIME_DECAY;

    private ESClient esClient;
    private Cache<String, List<DocItem>> localCache;
    private Cache<String, List<DocItem>> redisCache;

    @Override
    protected Map<String, Object> doBuildParam(QueryRequest queryRequest) {
        WorkerCoreQuery coreQuery = queryRequest.getCoreQuery();
        String oriQuery = coreQuery.getQuery();
        List<String> querySegments = coreQuery.getQuerySegments();
        String combinedQuery = String.join(" ", querySegments);

        Map<String, Object> param = new HashMap<>();
        param.put(Constants.ESConfig.INDEX_KEY, esIndex());
        param.put(Constants.ESConfig.FROM_KEY, 0);
        param.put(Constants.ESConfig.SIZE_KEY, 10);

        Map<String, Object> query = new HashMap<>();
        List<Map<String, Object>> must = new ArrayList<>();
        Map<String, Object> docMatch = new HashMap<>();
        docMatch.put(Constants.ESConfig.TYPE_KEY, Constants.ESConfig.BOOL_KEY);
        List<Map<String, Object>> docShould = new ArrayList<>();
        docShould.add(buildConditionItem(Constants.ESConfig.MATCH_PHRASE_KEY, "headline", oriQuery, 10D));
        docShould.add(buildConditionItem(Constants.ESConfig.MATCH_PHRASE_PREFIX_KEY, "titleSegs", combinedQuery, 10D));
        docMatch.put(Constants.ESConfig.SHOULD_KEY, docShould);
        must.add(docMatch);
        query.put(Constants.ESConfig.MUST_KEY, must);
        param.put(Constants.ESConfig.QUERY_KEY, query);

        return param;
    }

    protected BiFunction<QueryRequest, Map<String, Object>, Boolean> esDataFilter() {
        return (queryRequest, map) -> NewsUtils.esDataJudgement(defaultScore(), map);
    }

    protected DocItem docItemBuilder(Map<String, Object> map) {
        return NewsUtils.docItemBuilder(dataType(), defaultScore(), map);
    }

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
