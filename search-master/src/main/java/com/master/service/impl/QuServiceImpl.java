package com.master.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.common.OperatorConfigCache;
import com.common.SysConfigUtil;
import com.common.utils.ExtraCollectionUtils;
import com.common.utils.GuidService;
import com.github.benmanes.caffeine.cache.Cache;
import com.master.service.QuService;
import com.model.DocItem;
import com.model.input.*;
import com.service.base.BaseCacheAbleService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Service
public class QuServiceImpl extends BaseCacheAbleService<CoreQuery, QueryRequest> implements QuService {

    @Value("${SEARCH_MASTER_QU_HANDLER_KEY:SEARCH_MASTER_QU}")
    private String HANDLER_KEY;
    @Value("${SEARCH_MASTER_QU_OPERATOR_ACTIVE:true}")
    protected boolean OPERATOR_ACTIVE;

    @Value("${SEARCH_MASTER_QU_TIMEOUT:2000}")
    private int TIMEOUT;

    @Value("${SEARCH_MASTER_QU_REMOTE_URL:http://internalapi.aigauss.com/baymaxentity/api/entities}")
    private String REMOTE_URL;
    @Value("${SEARCH_MASTER_QU_TARGET_ITEM_KEY:segment,stock,pm_project}")
    private String TARGET_ITEM_KEY;

    @Value("${SEARCH_MASTER_QU_CACHE_TYPE:REDIS}")
    private String CACHE_TYPE;
    @Value("${SEARCH_MASTER_QU_CACHE_MAX_SIZE:1000}")
    private Integer CACHE_MAX_SIZE;
    @Value("${SEARCH_MASTER_QU_CACHE_CAPACITY:100}")
    private Integer CACHE_CAPACITY;
    @Value("${SEARCH_MASTER_QU_CACHE_EXPIRE_TIME:60}")
    private Integer CACHE_EXPIRE_TIME;

    @Value("${SEARCH_MASTER_QU_REDIS_HOST:redis}")
    private String REDIS_HOST;
    @Value("${SEARCH_MASTER_QU_REDIS_PORT:6379}")
    private Integer REDIS_PORT;
    @Value("${SEARCH_MASTER_QU_REDIS_PASSWORD:9a46259f1b75feaa}")
    private String REDIS_PASSWORD;
    @Value("${SEARCH_MASTER_QU_REDIS_DATABASE:0}")
    private Integer REDIS_DATABASE;

    @Value("${SEARCH_MASTER_QU_REDIS_PROTOCOL_TIMEOUT:2000}")
    private Integer REDIS_PROTOCOL_TIMEOUT;
    @Value("${SEARCH_MASTER_QU_REDIS_MAX_IDLE:3}")
    private Integer REDIS_MAX_IDLE;
    @Value("${SEARCH_MASTER_QU_REDIS_MAX_TOTAL:1000}")
    private Integer REDIS_MAX_TOTAL;
    @Value("${SEARCH_MASTER_QU_REDIS_MAX_WAIT_MILLIS:10}")
    private Integer REDIS_MAX_WAIT_MILLIS;

    @Value("${SEARCH_MASTER_QU_REDIS_MIN_EVICTABLE_IDLE_TIME_MILLIS:1000}")
    private Long REDIS_MIN_EVICTABLE_IDLE_TIME_MILLIS;
    @Value("${SEARCH_MASTER_QU_REDIS_NUM_TESTS_PER_EVICTION_RUN:3}")
    private Integer REDIS_NUM_TESTS_PER_EVICTION_RUN;
    @Value("${SEARCH_MASTER_QU_REDIS_TIME_BETWEEN_EVICTION_RUNS_MILLIS:3}")
    private Integer REDIS_TIME_BETWEEN_EVICTION_RUNS_MILLIS;
    @Value("${SEARCH_MASTER_QU_REDIS_TEST_ON_BORROW:true}")
    private Boolean REDIS_TEST_ON_BORROW;
    @Value("${SEARCH_MASTER_QU_REDIS_TEST_WHILE_IDLE:true}")
    private Boolean REDIS_TEST_WHILE_IDLE;

    private Cache<String, List<DocItem>> localCache;
    private Cache<String, List<DocItem>> redisCache;
    private Map<String, BiConsumer<WorkerCoreQuery, Map<String, Object>>> quOperators;

    @Override
    public QueryRequest defaultResult() {
        return new QueryRequest();
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

    public String cacheKey(CoreQuery coreQuery) {
        String hash = GuidService.getXXHash(coreQuery.getQuery());
        return String.format(cacheKeyPattern()
                , SysConfigUtil.getAsString("ServiceInstance", "INSTANCE_KEY", "SEARCH_MASTER_QU")
                , handlerKey()
                , hash
        );
    }

    @Override
    protected Map<String, Object> doBuildParam(CoreQuery query) {
        Map<String, Object> param = new HashMap<>();
        param.put("query", query.getQuery());
        param.put("types", targetItemsKey());
        return param;
    }

    private List<String> targetItemsKey() {
        return SysConfigUtil.getAsStringList(
                operatorName()
                , "TARGET_ITEM_KEY_LIST"
                , Stream.of(SysConfigUtil.getAsString(
                        operatorName()
                        , "TARGET_ITEM_KEY"
                        , TARGET_ITEM_KEY
                        ).split(",")
                ).collect(Collectors.toList())
        );
    }

    @Override
    protected Map<String, Object> doDataQuery(CoreQuery query, Map<String, Object> param) {
        return doRemoteDataQuery(query, param);
    }

    protected boolean isLegalRemoteResult(Map<String, Object> remoteResultMap) {
        List<List> targetItemLists = targetItemsKey().stream()
                .map(remoteResultMap::get)
                .filter(ObjectUtils::isNotEmpty)
                .filter(o -> o instanceof List)
                .map(o -> (List) o)
                .filter(ExtraCollectionUtils::isNotEmpty)
                .collect(Collectors.toList());
        return super.isLegalRemoteResult(remoteResultMap) &&
                ExtraCollectionUtils.isNoneEmpty(targetItemLists);
    }

    protected Map<String, Object> parseRemoteResult(Map<String, Object> remoteResultMap) {
        return remoteResultMap;
    }

    @Override
    protected QueryRequest doParseResult(CoreQuery query, Map<String, Object> queryResultMap) {

        WorkerCoreQuery coreQuery = buildCoreQuery(query, queryResultMap);
        QueryConfig queryConfig = buildQueryConfig(query, queryResultMap);
        QueryRequest result = QueryRequest.builder()
                .coreQuery(coreQuery)
                .queryConfig(queryConfig)
                .build();

        log.info("Got Qu result:[{}] for query:[{}]", JSON.toJSONString(result), query.getQuery());
        return result;
    }

    private QueryConfig buildQueryConfig(CoreQuery query, Map<String, Object> queryResultMap) {
        return QueryConfig.builder()
                .globalRate(confidence())
                .rateMapping(OperatorConfigCache.loadAll())
                .build();
    }

    private WorkerCoreQuery buildCoreQuery(CoreQuery query, Map<String, Object> queryResultMap) {
        WorkerCoreQuery coreQuery = new WorkerCoreQuery(query);
        quOperators.values().forEach(biConsumer -> biConsumer.accept(coreQuery, queryResultMap));
        return coreQuery;
    }

    private BiConsumer<WorkerCoreQuery, Map<String, Object>> segmentConsumer() {
        return (coreQuery, map) -> {
            List<Map<String, Object>> list = (List<Map<String, Object>>) map.getOrDefault("segment", new ArrayList<>());
            List<String> segments = list.stream()
                    .map(x -> (String) x.getOrDefault("text", ""))
                    .map(String::trim)
                    .filter(StringUtils::isNotBlank)
                    .collect(Collectors.toList());
            coreQuery.setQuerySegments(segments);
        };
    }

    private BiConsumer<WorkerCoreQuery, Map<String, Object>> stockConsumer() {
        return (coreQuery, map) -> {
            List<Map<String, Object>> list = (List<Map<String, Object>>) map.getOrDefault("stock", new ArrayList<>());
            List<StockInfo> stockInfo = list.stream()
                    .map(item -> {
                        int id = (Integer) item.getOrDefault("id", -1);
                        String code = (String) item.getOrDefault("code", "NA");
                        String market = (String) item.getOrDefault("ext", "NA");
                        String stockId = String.format("%s.%s", code, market).toUpperCase();
                        String name = (String) item.getOrDefault("name", "NA");
                        return StockInfo.builder()
                                .id(id)
                                .code(code)
                                .name(name)
                                .market(market)
                                .stockId(stockId)
                                .build();
                    }).filter(item -> 0 < item.getId()
                            && StringUtils.isNoneBlank(item.getCode(), item.getMarket()))
                    .collect(Collectors.toList());
            coreQuery.setStockList(stockInfo);
        };
    }

    private BiConsumer<WorkerCoreQuery, Map<String, Object>> projectConsumer() {
        return (coreQuery, map) -> {
            List<Map<String, Object>> list = (List<Map<String, Object>>) map.getOrDefault("pm_project", new ArrayList<>());
            List<ProjectInfo> projectInfo = list.stream()
                    .map(item -> {
                                int id = (Integer) item.getOrDefault("id", -1);
                                String code = (String) item.getOrDefault("code", "NA");
                                String name = (String) item.getOrDefault("name", "NA");
                                return ProjectInfo.builder()
                                        .id(id)
                                        .code(code)
                                        .name(name)
                                        .build();
                            }
                    ).filter(item -> 0 < item.getId()
                            && StringUtils.isNoneBlank(item.getCode(), item.getName()))
                    .collect(Collectors.toList());
            coreQuery.setProjectList(projectInfo);
        };
    }

    @Override
    protected Function<String, QueryRequest> redisCacheLoadConverter() {
        return string -> JSONObject.parseObject(string, QueryRequest.class);
    }

    @Override
    protected Predicate<QueryRequest> cacheDataPredicate() {
        return config -> super.cacheDataPredicate().test(config)
                && !(
                ObjectUtils.isEmpty(config.getCoreQuery())
                        || CollectionUtils.isEmpty(config.getCoreQuery().getQuerySegments())
                        || ObjectUtils.isEmpty(config.getQueryConfig())
                        || CollectionUtils.isEmpty(config.getQueryConfig().getRateMapping())
        );
    }

    @Override
    protected Function<QueryRequest, String> redisCacheSaveConverter() {
        return JSONObject::toJSONString;
    }

    @Override
    public Map<String, Object> findConfig() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("HANDLER_KEY", HANDLER_KEY);
        config.put("OPERATOR_ACTIVE", OPERATOR_ACTIVE);
        config.put("TIMEOUT", TIMEOUT);

        config.put("REMOTE_URL", REMOTE_URL);

        config.put("TARGET_ITEM_KEY", TARGET_ITEM_KEY);
        List<String> targetItemKeyList = Stream.of(TARGET_ITEM_KEY.split(",")).collect(Collectors.toList());
        config.put("TARGET_ITEM_KEY_LIST", targetItemKeyList);

        config.put("CACHE_TYPE", CACHE_TYPE);
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
        return config;
    }

    @PostConstruct
    protected void init() {
        super.init();
        quOperators = new HashMap<>();
        quOperators.put("stock", stockConsumer());
        quOperators.put("segment", segmentConsumer());
        quOperators.put("pm_project", projectConsumer());
    }

}