package com.earning.service.operators;

import com.earning.service.impl.V6ESClient;
import com.common.ESClientUtil;
import com.common.SysConfigUtil;
import com.common.constants.Constants.ESConfig;
import com.common.datasource.ESClient;
import com.common.utils.ExtraCollectionUtils;
import com.github.benmanes.caffeine.cache.Cache;
import com.model.DocItem;
import com.model.input.QueryRequest;
import com.model.input.WorkerCoreQuery;
import com.service.worker.operators.WorkerDocGetOperator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class EarningDocGetOperator extends WorkerDocGetOperator {

    @Value("${EARNING_DOC_GET_HANDLER_KEY:EARNING_DOC_GET}")
    private String HANDLER_KEY;
    @Value("${EARNING_DOC_GET_OPERATOR_ACTIVE:true}")
    private boolean OPERATOR_ACTIVE;

    @Value("${EARNING_DOC_GET_DATA_TYPE:doc}")
    private String DATA_TYPE;

    @Value("${EARNING_DOC_GET_OPERATOR_TIMEOUT:2000}")
    private int TIMEOUT;

    @Value("${EARNING_DOC_GET_CACHE_TYPE:REDIS}")
    private String CACHE_TYPE;

    @Value("${EARNING_DOC_GET_ES_ADDRESS:localhost:9200}")
    private String ES_ADDRESS;
    @Value("${EARNING_DOC_GET_ES_USERNAME:}")
    private String ES_USERNAME;
    @Value("${EARNING_DOC_GET_ES_PASSWORD:}")
    private String ES_PASSWORD;
    @Value("${EARNING_DOC_GET_ES_INDEX:}")
    private String ES_INDEX;

    @Value("${EARNING_DOC_GET_DECAY_TYPE:HYPERBOLIC}")
    private String DECAY_TYPE;
    @Value("${EARNING_DOC_GET_TIME_DECAY:0.4}")
    private double TIME_DECAY;

    private ESClient esClient;
    private Cache<String, List<DocItem>> localCache;
    private Cache<String, List<DocItem>> redisCache;

    @Override
    @PostConstruct
    protected void init() {
        super.init();
        esClient = new V6ESClient(buildESConfigBean());
        ESClientUtil.set(handlerKey(), esClient);
    }

    @Override
    public boolean invalidatedParam(QueryRequest param) {
        return super.invalidatedParam(param)
                || ExtraCollectionUtils.isAllEmpty(param.getCoreQuery().getStockList(), param.getCoreQuery().getProjectList());
    }

    @Override
    protected DocItem docItemBuilder(Map<String, Object> map) {
        DocItem docItem = super.docItemBuilder(map);
        docItem.setFuncId((String) map.getOrDefault(ESConfig.ES_ID, ""));
        return docItem;
    }

    @Override
    protected Map<String, Object> doBuildParam(QueryRequest queryRequest) {
        WorkerCoreQuery coreQuery = queryRequest.getCoreQuery();
        String oriQuery = coreQuery.getQuery();
        List<String> querySegments = coreQuery.getQuerySegments();
        String combinedQuery = String.join(" ", querySegments);

        Map<String, Object> param = new HashMap<>();
        param.put(ESConfig.INDEX_KEY, esIndex());
        param.put(ESConfig.FROM_KEY, 0);
        param.put(ESConfig.SIZE_KEY, 10);

        Map<String, Object> query = new HashMap<>();
        List<Map<String, Object>> must = new ArrayList<>();
        Map<String, Object> docMatch = new HashMap<>();
        docMatch.put(ESConfig.TYPE_KEY, ESConfig.BOOL_KEY);
        List<Map<String, Object>> docShould = new ArrayList<>();
        docShould.add(buildConditionItem(ESConfig.MATCH_KEY, "title", oriQuery, 10D));
        docShould.add(buildConditionItem(ESConfig.MATCH_PHRASE_PREFIX_KEY, "titleSeg", combinedQuery, 10D));
        docMatch.put(ESConfig.SHOULD_KEY, docShould);
        must.add(docMatch);
        query.put(ESConfig.MUST_KEY, must);
        param.put(ESConfig.QUERY_KEY, query);

        return param;
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
    protected ESClient esClient() { return this.esClient; }

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
