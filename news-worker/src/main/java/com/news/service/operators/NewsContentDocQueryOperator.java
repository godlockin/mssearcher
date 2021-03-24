package com.news.service.operators;

import com.common.SysConfigUtil;
import com.common.constants.Constants.ESConfig;
import com.common.datasource.ESClient;
import com.github.benmanes.caffeine.cache.Cache;
import com.model.DocItem;
import com.model.input.QueryRequest;
import com.model.input.WorkerCoreQuery;
import com.news.common.NewsUtils;
import com.service.worker.operators.WorkerDocQueryOperator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

@Slf4j
@Service
public class NewsContentDocQueryOperator extends WorkerDocQueryOperator {

    @Value("${NEWS_CONTENT_DOC_QUERY_HANDLER_KEY:NEWS_CONTENT_DOC_QUERY}")
    private String HANDLER_KEY;
    @Value("${NEWS_CONTENT_DOC_QUERY_OPERATOR_ACTIVE:true}")
    private boolean OPERATOR_ACTIVE;

    @Value("${NEWS_CONTENT_DOC_QUERY_CONFIDENCE:0.2}")
    protected double CONFIDENCE;
    @Value("${NEWS_CONTENT_DOC_QUERY_DATA_TYPE:content}")
    private String DATA_TYPE;

    @Value("${NEWS_CONTENT_DOC_QUERY_OPERATOR_TIMEOUT:2000}")
    private int TIMEOUT;

    @Value("${NEWS_CONTENT_DOC_QUERY_CACHE_TYPE:REDIS}")
    protected String CACHE_TYPE;

    @Value("${NEWS_CONTENT_DOC_QUERY_ES_ADDRESS:localhost:9200}")
    protected String ES_ADDRESS;
    @Value("${NEWS_CONTENT_DOC_QUERY_ES_USERNAME:}")
    protected String ES_USERNAME;
    @Value("${NEWS_CONTENT_DOC_QUERY_ES_PASSWORD:}")
    protected String ES_PASSWORD;
    @Value("${NEWS_CONTENT_DOC_QUERY_ES_INDEX:}")
    protected String ES_INDEX;

    @Value("${NEWS_CONTENT_DOC_QUERY_DECAY_TYPE:HYPERBOLIC}")
    private String DECAY_TYPE;
    @Value("${NEWS_CONTENT_DOC_QUERY_TIME_DECAY:0.8}")
    private double TIME_DECAY;

    private ESClient esClient;
    private Cache<String, List<DocItem>> localCache;
    private Cache<String, List<DocItem>> redisCache;

    @Override
    protected Map<String, Object> doBuildQuery(QueryRequest queryRequest) {
        WorkerCoreQuery coreQuery = queryRequest.getCoreQuery();
        String oriQuery = coreQuery.getQuery();
        List<String> querySegments = coreQuery.getQuerySegments();
        String combinedQuery = String.join(" ", querySegments);

        Map<String, Object> query = new HashMap<>();
        List<Map<String, Object>> must = new ArrayList<>();
        Map<String, Object> docMatch = new HashMap<>();
        docMatch.put(ESConfig.TYPE_KEY, ESConfig.BOOL_KEY);
        List<Map<String, Object>> docShould = new ArrayList<>();
        docShould.add(buildConditionItem(ESConfig.MATCH_PHRASE_KEY, "headline", oriQuery, 8D));
        docShould.add(buildConditionItem(ESConfig.MATCH_PHRASE_KEY, "titleSegs", combinedQuery, 5D));

        docShould.add(buildConditionItem(ESConfig.MATCH_PHRASE_KEY, "content", combinedQuery, 5D));

        coreQuery.getStockList().forEach(stockInfo -> docShould.add(buildConditionItem(ESConfig.MATCH_KEY, "stockList", stockInfo.getCode(), 15D)));
        coreQuery.getProjectList().forEach(projectInfo -> docShould.add(buildConditionItem(ESConfig.MATCH_KEY, "projectList", projectInfo.getId(), 15D)));

        docMatch.put(ESConfig.SHOULD_KEY, docShould);
        must.add(docMatch);
        query.put(ESConfig.MUST_KEY, must);
        return query;
    }

    private Map<String, Object> buildQuery(String tagType, Object value, Double boost) {
        Map<String, Object> param = new HashMap<>();
        param.put(ESConfig.TYPE_KEY, ESConfig.BOOL_KEY);

        List<Map<String, Object>> must = new ArrayList<>();
        Map<String, Object> tagTypeMatch = new HashMap<>();
        tagTypeMatch.put(ESConfig.TYPE_KEY, ESConfig.NESTED_KEY);
        tagTypeMatch.put(ESConfig.PATH_KEY, "annotationList");
        Map<String, Object> tagQuery = new HashMap<>();
        tagQuery.put(ESConfig.TYPE_KEY, ESConfig.MATCH_KEY);
        tagQuery.put(ESConfig.FIELD_KEY, "annotationList.uid");
        tagQuery.put(ESConfig.VALUE_KEY, tagType);
        tagTypeMatch.put(ESConfig.QUERY_KEY, tagQuery);
        must.add(tagTypeMatch);

        Map<String, Object> tagIdMatch = new HashMap<>();
        tagIdMatch.put(ESConfig.TYPE_KEY, ESConfig.NESTED_KEY);
        tagIdMatch.put(ESConfig.PATH_KEY, "annotationList.tagList");
        Map<String, Object> tagIdQuery = new HashMap<>();
        tagIdQuery.put(ESConfig.TYPE_KEY, ESConfig.MATCH_KEY);
        tagIdQuery.put(ESConfig.FIELD_KEY, "annotationList.tagList.uid");
        tagIdQuery.put(ESConfig.VALUE_KEY, value);
        tagIdMatch.put(ESConfig.QUERY_KEY, tagIdQuery);
        tagIdMatch.put(ESConfig.BOOST_KEY, boost);
        must.add(tagIdMatch);
        param.put(ESConfig.MUST_KEY, must);

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
