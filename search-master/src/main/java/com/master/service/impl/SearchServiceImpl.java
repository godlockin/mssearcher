package com.master.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONValidator;
import com.common.SysConfigUtil;
import com.common.utils.DataUtils;
import com.common.utils.ExtraCollectionUtils;
import com.common.utils.GuidService;
import com.github.benmanes.caffeine.cache.Cache;
import com.master.service.QuService;
import com.master.service.SearchService;
import com.master.service.operators.WorkerCallbackOperator;
import com.model.DocItem;
import com.model.SortItem;
import com.model.input.QueryConfig;
import com.model.input.QueryRequest;
import com.model.input.WorkerCoreQuery;
import com.model.internal.Pair;
import com.service.base.BaseCacheAbleService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Slf4j
@Service
public class SearchServiceImpl extends BaseCacheAbleService<QueryRequest, List<SortItem>> implements SearchService {

    @Value("${SEARCH_MASTER_SEARCH_HANDLER_KEY:SEARCH_MASTER_SEARCHER}")
    private String HANDLER_KEY;
    @Value("${SEARCH_MASTER_OPERATOR_ACTIVE:true}")
    private boolean OPERATOR_ACTIVE;

    @Value("${SEARCH_MASTER_SEARCH_TIMEOUT:2000}")
    private int TIMEOUT;

    @Value("${SEARCH_MASTER_CACHE_TYPE:REDIS}")
    private String CACHE_TYPE;
    @Value("${SEARCH_MASTER_CACHE_MAX_SIZE:1000}")
    private Integer CACHE_MAX_SIZE;
    @Value("${SEARCH_MASTER_CACHE_CAPACITY:100}")
    private Integer CACHE_CAPACITY;
    @Value("${SEARCH_MASTER_CACHE_EXPIRE_TIME:60}")
    private Integer CACHE_EXPIRE_TIME;

    @Value("${SEARCH_MASTER_REDIS_HOST:redis}")
    private String REDIS_HOST;
    @Value("${SEARCH_MASTER_REDIS_PORT:6379}")
    private Integer REDIS_PORT;
    @Value("${SEARCH_MASTER_REDIS_PASSWORD:9a46259f1b75feaa}")
    private String REDIS_PASSWORD;
    @Value("${SEARCH_MASTER_REDIS_DATABASE:0}")
    private Integer REDIS_DATABASE;

    @Value("${SEARCH_MASTER_REDIS_PROTOCOL_TIMEOUT:2000}")
    private Integer REDIS_PROTOCOL_TIMEOUT;
    @Value("${SEARCH_MASTER_REDIS_MAX_IDLE:3}")
    private Integer REDIS_MAX_IDLE;
    @Value("${SEARCH_MASTER_REDIS_MAX_TOTAL:1000}")
    private Integer REDIS_MAX_TOTAL;
    @Value("${SEARCH_MASTER_REDIS_MAX_WAIT_MILLIS:10}")
    private Integer REDIS_MAX_WAIT_MILLIS;

    @Value("${SEARCH_MASTER_REDIS_MIN_EVICTABLE_IDLE_TIME_MILLIS:1000}")
    private Long REDIS_MIN_EVICTABLE_IDLE_TIME_MILLIS;
    @Value("${SEARCH_MASTER_REDIS_NUM_TESTS_PER_EVICTION_RUN:3}")
    private Integer REDIS_NUM_TESTS_PER_EVICTION_RUN;
    @Value("${SEARCH_MASTER_REDIS_TIME_BETWEEN_EVICTION_RUNS_MILLIS:3}")
    private Integer REDIS_TIME_BETWEEN_EVICTION_RUNS_MILLIS;
    @Value("${SEARCH_MASTER_REDIS_TEST_ON_BORROW:true}")
    private Boolean REDIS_TEST_ON_BORROW;
    @Value("${SEARCH_MASTER_REDIS_TEST_WHILE_IDLE:true}")
    private Boolean REDIS_TEST_WHILE_IDLE;

    @Autowired
    private QuService quService;
    @Autowired
    private Map<String, WorkerCallbackOperator> workerOperators;

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
    public List<SortItem> defaultResult() {
        return new ArrayList<>();
    }

    @Override
    public boolean invalidatedParam(QueryRequest queryRequest) {
        return super.invalidatedParam(queryRequest)
                || ObjectUtils.isEmpty(queryRequest.getCoreQuery())
                || StringUtils.isBlank(queryRequest.getCoreQuery().getQuery())
                || 0 >= queryRequest.getCoreQuery().getPageNo()
                || 0 >= queryRequest.getCoreQuery().getPageSize();
    }

    @Override
    public String cacheKey(QueryRequest queryRequest) {
        WorkerCoreQuery coreQuery = queryRequest.getCoreQuery();
        List<String> keyItems = new ArrayList<>(coreQuery.getDataSources());
        String hash = GuidService.getXXHash(coreQuery.getQuery());
        keyItems.add(hash);
        String wholeKey = String.join(",", keyItems);
        return String.format(cacheKeyPattern()
                , SysConfigUtil.getAsString("ServiceInstance", "INSTANCE_KEY", "SEARCH_MASTER")
                , handlerKey()
                , wholeKey
        );
    }

    @Override
    public List<SortItem> handle(QueryRequest param) {
        List<SortItem> docList = super.handle(param);
        if (CollectionUtils.isEmpty(docList)) {
            return docList;
        }

        WorkerCoreQuery coreQuery = param.getCoreQuery();
        List<SortItem> outList = DataUtils.handlePaging(coreQuery.getPageNo(), coreQuery.getPageSize(), docList);
        log.info("Built {} docs", outList.size());
        return outList;
    }

    @Override
    protected Map<String, Object> doBuildParam(QueryRequest query) {
        QueryRequest queryRequest = quService.handle(query.getCoreQuery());
        BeanUtils.copyProperties(queryRequest, query);
        return Collections.emptyMap();
    }

    @Override
    protected Map<String, Object> doDataQuery(QueryRequest query, Map<String, Object> param) {
        WorkerCoreQuery coreQuery = query.getCoreQuery();
        List<String> dataSources = CollectionUtils.isEmpty(coreQuery.getDataSources()) ?
                Collections.singletonList("NEWS_CALLBACK") : coreQuery.getDataSources();
        Map<String, Object> resultMap = workerOperators.values().parallelStream()
                .filter(operator -> dataSources.contains(operator.handlerKey()))
                .filter(WorkerCallbackOperator::operatorActive)
                .map(operator -> Pair.of(operator.operatorName(), operator.handle(query)))
                .collect(Collectors.toConcurrentMap(Pair::getKey, Pair::getValue));
        log.info("Built {} groups of results", resultMap.size());
        return resultMap;
    }

    @Override
    protected List<SortItem> doParseResult(QueryRequest query, Map<String, Object> queryResultMap) {
        QueryConfig queryConfig = query.getQueryConfig();

        Set<String> distinct = ConcurrentHashMap.newKeySet();
        List<SortItem> docList = queryResultMap.entrySet().parallelStream()
                .filter(e -> e.getValue() instanceof List)
                .map(e -> (List<SortItem>) e.getValue())
                .filter(ExtraCollectionUtils::isNotEmpty)
                .flatMap(Collection::stream)
                .peek(sortItem -> {
                    String dataType = sortItem.getDataType();
                    double rate = queryConfig.getRateMapping().getOrDefault(dataType, 1D);
                    if (sortItem.isProbablyMatch()) {
                        rate *= 10;
                    }
                    sortItem.setScore(sortItem.getScore() * rate);
                })
                .sorted(Comparator.comparing(SortItem::getScore).reversed())
                .filter(sortItem -> distinct.add(sortItem.getBundleKey()))
                .collect(Collectors.toList());
        log.info("Got {} docs", docList.size());
        return docList;
    }

    @Override
    protected Predicate<String> redisCachePredicate() {
        return content -> StringUtils.isNotBlank(content) && JSONValidator.from(content).validate();
    }

    @Override
    protected Function<String, List<SortItem>> redisCacheLoadConverter() {
        return string -> JSONArray.parseArray(string, SortItem.class);
    }

    @Override
    protected Predicate<List<SortItem>> cacheDataPredicate() {
        return ExtraCollectionUtils::isNotEmpty;
    }

    @Override
    protected Function<List<SortItem>, String> redisCacheSaveConverter() {
        return JSONArray::toJSONString;
    }

    @Override
    @PostConstruct
    protected void init() {
        super.init();
    }

    @Override
    public Map<String, Object> findConfig() {
        Map<String, Object> config = super.findConfig();
        config.put("OPERATOR_ACTIVE", OPERATOR_ACTIVE);
        config.put("HANDLER_KEY", HANDLER_KEY);
        config.put("TIMEOUT", TIMEOUT);

        config.put("CACHE_TYPE", CACHE_TYPE);
        config.put("CACHE_MAX_SIZE", CACHE_MAX_SIZE);
        config.put("CACHE_CAPACITY", CACHE_CAPACITY);
        config.put("CACHE_EXPIRE_TIME", CACHE_EXPIRE_TIME);

        config.put("REDIS_HOST", REDIS_HOST);
        config.put("REDIS_PORT", REDIS_PORT);
        config.put("REDIS_PASSWORD", REDIS_PASSWORD);
        config.put("REDIS_DATABASE", REDIS_DATABASE);

        config.put("REDIS_MAX_IDLE", REDIS_MAX_IDLE);
        config.put("REDIS_MAX_TOTAL", REDIS_MAX_TOTAL);
        config.put("REDIS_TEST_ON_BORROW", REDIS_TEST_ON_BORROW);
        config.put("REDIS_TEST_WHILE_IDLE", REDIS_TEST_WHILE_IDLE);
        config.put("REDIS_MAX_WAIT_MILLIS", REDIS_MAX_WAIT_MILLIS);
        config.put("REDIS_PROTOCOL_TIMEOUT", REDIS_PROTOCOL_TIMEOUT);
        config.put("REDIS_NUM_TESTS_PER_EVICTION_RUN", REDIS_NUM_TESTS_PER_EVICTION_RUN);
        config.put("REDIS_MIN_EVICTABLE_IDLE_TIME_MILLIS", REDIS_MIN_EVICTABLE_IDLE_TIME_MILLIS);
        config.put("REDIS_TIME_BETWEEN_EVICTION_RUNS_MILLIS", REDIS_TIME_BETWEEN_EVICTION_RUNS_MILLIS);
        return config;
    }
}