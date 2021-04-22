package com.service.worker.operators;

import com.alibaba.fastjson.JSONArray;
import com.common.SysConfigUtil;
import com.common.constants.Constants.ESConfig;
import com.model.DocItem;
import com.model.input.QueryRequest;
import com.model.input.WorkerCoreQuery;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Function;

@Slf4j
@Service
public abstract class WorkerDocQueryOperator extends WorkerSearchOperator {

    @Value("${DOC_QUERY_CONFIDENCE:1.0}")
    protected double CONFIDENCE;
    @Value("${DOC_QUERY_DEFAULT_SCORE:0.0}")
    protected double DEFAULT_SCORE;

    /**
     * common cache config
     */
    @Value("${DOC_QUERY_CACHE_MAX_SIZE:1000}")
    protected Integer CACHE_MAX_SIZE;
    @Value("${DOC_QUERY_CACHE_CAPACITY:100}")
    protected Integer CACHE_CAPACITY;
    @Value("${DOC_QUERY_CACHE_EXPIRE_TIME:60}")
    protected Integer CACHE_EXPIRE_TIME;

    /**
     * redis config
     */
    @Value("${DOC_QUERY_REDIS_HOST:redis}")
    protected String REDIS_HOST;
    @Value("${DOC_QUERY_REDIS_PORT:6379}")
    protected Integer REDIS_PORT;
    @Value("${DOC_QUERY_REDIS_PASSWORD:9a46259f1b75feaa}")
    protected String REDIS_PASSWORD;
    @Value("${DOC_QUERY_REDIS_DATABASE:0}")
    protected Integer REDIS_DATABASE;

    @Value("${DOC_QUERY_REDIS_PROTOCOL_TIMEOUT:2000}")
    protected Integer REDIS_PROTOCOL_TIMEOUT;
    @Value("${DOC_QUERY_REDIS_MAX_IDLE:3}")
    protected Integer REDIS_MAX_IDLE;
    @Value("${DOC_QUERY_REDIS_MAX_TOTAL:1000}")
    protected Integer REDIS_MAX_TOTAL;
    @Value("${DOC_QUERY_REDIS_MAX_WAIT_MILLIS:10}")
    protected Integer REDIS_MAX_WAIT_MILLIS;
    @Value("${DOC_QUERY_REDIS_MIN_EVICTABLE_IDLE_TIME_MILLIS:1000}")
    protected Long REDIS_MIN_EVICTABLE_IDLE_TIME_MILLIS;
    @Value("${DOC_QUERY_REDIS_NUM_TESTS_PER_EVICTION_RUN:3}")
    protected Integer REDIS_NUM_TESTS_PER_EVICTION_RUN;
    @Value("${DOC_QUERY_REDIS_TIME_BETWEEN_EVICTION_RUNS_MILLIS:3}")
    protected Integer REDIS_TIME_BETWEEN_EVICTION_RUNS_MILLIS;
    @Value("${DOC_QUERY_REDIS_TEST_ON_BORROW:true}")
    protected Boolean REDIS_TEST_ON_BORROW;
    @Value("${DOC_QUERY_REDIS_TEST_WHILE_IDLE:true}")
    protected Boolean REDIS_TEST_WHILE_IDLE;

    /**
     * ES config
     */
    @Value("${DOC_QUERY_ES_ADDRESS:localhost:9200}")
    protected String ES_ADDRESS;
    @Value("${DOC_QUERY_ES_USERNAME:}")
    protected String ES_USERNAME;
    @Value("${DOC_QUERY_ES_PASSWORD:}")
    protected String ES_PASSWORD;

    @Value("${DOC_QUERY_ES_BULK_SIZE:10}")
    protected Integer ES_BULK_SIZE;
    @Value("${DOC_QUERY_ES_BULK_FLUSH:5000}")
    protected Integer ES_BULK_FLUSH;
    @Value("${DOC_QUERY_ES_SOCKET_TIMEOUT:40000}")
    protected Integer ES_SOCKET_TIMEOUT;
    @Value("${DOC_QUERY_ES_BULK_CONCURRENT:3}")
    protected Integer ES_BULK_CONCURRENT;
    @Value("${DOC_QUERY_ES_CONNECT_TIMEOUT:5000}")
    protected Integer ES_CONNECT_TIMEOUT;
    @Value("${DOC_QUERY_ES_CONNECTION_REQUEST_TIMEOUT:1000}")
    protected Integer ES_CONNECTION_REQUEST_TIMEOUT;

    @Override
    protected Map<String, Object> doBuildParam(QueryRequest queryRequest) {
        Map<String, Object> param = doBuildBaseParam(queryRequest);

        Map<String, Object> query = doBuildQuery(queryRequest);
        param.put(ESConfig.QUERY_KEY, query);

        return param;
    }

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
        docShould.add(buildConditionItem(ESConfig.MATCH_KEY, "title", oriQuery, 5D));
        docShould.add(buildConditionItem(ESConfig.MATCH_PHRASE_KEY, "title", oriQuery, 8D));
        docShould.add(buildConditionItem(ESConfig.MATCH_PHRASE_KEY, "titleSegs", combinedQuery, 5D));

        docShould.add(buildConditionItem(ESConfig.MATCH_KEY, "content", combinedQuery, 3D));
        docShould.add(buildConditionItem(ESConfig.MATCH_PHRASE_KEY, "content", combinedQuery, 5D));

        coreQuery.getStockList().forEach(stockInfo -> docShould.add(buildConditionItem(ESConfig.MATCH_KEY, "stockCodes", stockInfo.getCode(), 15D)));
        coreQuery.getProjectList().forEach(projectInfo -> docShould.add(buildConditionItem(ESConfig.MATCH_KEY, "projects", projectInfo.getId(), 15D)));

        docMatch.put(ESConfig.SHOULD_KEY, docShould);
        must.add(docMatch);
        query.put(ESConfig.MUST_KEY, must);
        return query;
    }

    protected Map<String, Object> doBuildBaseParam(QueryRequest queryRequest) {
        WorkerCoreQuery coreQuery = queryRequest.getCoreQuery();
        Map<String, Object> param = new HashMap<>();
        param.put(ESConfig.INDEX_KEY, esIndex());
        param.put(ESConfig.FROM_KEY, 0);
        param.put(ESConfig.SIZE_KEY, Math.max(coreQuery.getPageNo() * coreQuery.getPageSize(), 100));
        return param;
    }

    @Override
    protected Function<String, List<DocItem>> redisCacheLoadConverter() {
        return string -> JSONArray.parseArray(string, DocItem.class);
    }

    @Override
    protected Function<List<DocItem>, String> redisCacheSaveConverter() {
        return JSONArray::toJSONString;
    }

    @Override
    protected double defaultScore() {
        return SysConfigUtil.getAsDouble("DEFAULT_SCORE", DEFAULT_SCORE);
    }

    @Override
    protected DocItem docItemBuilder(Map<String, Object> map) {
        DocItem docItem = super.docItemBuilder(map);
        docItem.setFuncId((String) map.getOrDefault("id", ""));
        return docItem;
    }

    @Override
    public Map<String, Object> findConfig() {
        Map<String, Object> config = new LinkedHashMap<>();

        config.put("CONFIDENCE", CONFIDENCE);
        config.put("DEFAULT_SCORE", DEFAULT_SCORE);

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

        config.put("ES_ADDRESS", ES_ADDRESS);
        config.put("ES_USERNAME", ES_USERNAME);
        config.put("ES_PASSWORD", ES_PASSWORD);

        config.put("ES_BULK_SIZE", ES_BULK_SIZE);
        config.put("ES_BULK_FLUSH", ES_BULK_FLUSH);
        config.put("ES_SOCKET_TIMEOUT", ES_SOCKET_TIMEOUT);
        config.put("ES_BULK_CONCURRENT", ES_BULK_CONCURRENT);
        config.put("ES_CONNECT_TIMEOUT", ES_CONNECT_TIMEOUT);
        config.put("ES_ES_CONNECTION_REQUEST_TIMEOUT", ES_CONNECTION_REQUEST_TIMEOUT);
        return config;
    }
}
