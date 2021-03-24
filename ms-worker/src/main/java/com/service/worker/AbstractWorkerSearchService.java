package com.service.worker;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONValidator;
import com.common.SysConfigUtil;
import com.common.utils.DataUtils;
import com.common.utils.ExtraCollectionUtils;
import com.common.utils.GuidService;
import com.model.DocItem;
import com.model.SortItem;
import com.model.input.QueryConfig;
import com.model.input.QueryRequest;
import com.model.input.WorkerCoreQuery;
import com.model.internal.Pair;
import com.service.WorkerQuServiceInterface;
import com.service.worker.operators.WorkerSearchOperator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Slf4j
@Service
public abstract class AbstractWorkerSearchService extends AbstractWorkerCacheAbleService<QueryRequest, List<SortItem>> {

    @Value("${SEARCH_INSTANCE_KEY:GLOBAL_SEARCH_WORKER}")
    protected String INSTANCE_KEY;

    @Value("${SEARCH_WORKER_HANDLER_KEY:GLOBAL_SEARCH}")
    protected String HANDLER_KEY;
    @Value("${SEARCH_WORKER_OPERATOR_ACTIVE:true}")
    protected boolean OPERATOR_ACTIVE;

    @Value("${SEARCH_WORKER_CONFIDENCE:1.0}")
    protected double CONFIDENCE;

    @Value("${SEARCH_WORKER_CACHE_TYPE:REDIS}")
    protected String CACHE_TYPE;
    @Value("${SEARCH_WORKER_CACHE_MAX_SIZE:1000}")
    protected Integer CACHE_MAX_SIZE;
    @Value("${SEARCH_WORKER_CACHE_CAPACITY:100}")
    protected Integer CACHE_CAPACITY;
    @Value("${SEARCH_WORKER_CACHE_EXPIRE_TIME:60}")
    protected Integer CACHE_EXPIRE_TIME;

    @Value("${SEARCH_WORKER_REDIS_HOST:redis}")
    protected String REDIS_HOST;
    @Value("${SEARCH_WORKER_REDIS_PORT:6379}")
    protected Integer REDIS_PORT;
    @Value("${SEARCH_WORKER_REDIS_PASSWORD:9a46259f1b75feaa}")
    protected String REDIS_PASSWORD;
    @Value("${SEARCH_WORKER_REDIS_DATABASE:0}")
    protected Integer REDIS_DATABASE;

    @Value("${SEARCH_WORKER_REDIS_PROTOCOL_TIMEOUT:2000}")
    protected Integer REDIS_PROTOCOL_TIMEOUT;
    @Value("${SEARCH_WORKER_REDIS_MAX_IDLE:3}")
    protected Integer REDIS_MAX_IDLE;
    @Value("${SEARCH_WORKER_REDIS_MAX_TOTAL:1000}")
    protected Integer REDIS_MAX_TOTAL;
    @Value("${SEARCH_WORKER_REDIS_MAX_WAIT_MILLIS:10}")
    protected Integer REDIS_MAX_WAIT_MILLIS;

    @Value("${SEARCH_WORKER_REDIS_MIN_EVICTABLE_IDLE_TIME_MILLIS:1000}")
    protected Long REDIS_MIN_EVICTABLE_IDLE_TIME_MILLIS;
    @Value("${SEARCH_WORKER_REDIS_NUM_TESTS_PER_EVICTION_RUN:3}")
    protected Integer REDIS_NUM_TESTS_PER_EVICTION_RUN;
    @Value("${SEARCH_WORKER_REDIS_TIME_BETWEEN_EVICTION_RUNS_MILLIS:3}")
    protected Integer REDIS_TIME_BETWEEN_EVICTION_RUNS_MILLIS;
    @Value("${SEARCH_WORKER_REDIS_TEST_ON_BORROW:true}")
    protected Boolean REDIS_TEST_ON_BORROW;
    @Value("${SEARCH_WORKER_REDIS_TEST_WHILE_IDLE:true}")
    protected Boolean REDIS_TEST_WHILE_IDLE;

    protected abstract Map<String, WorkerSearchOperator> operators();

    protected abstract WorkerQuServiceInterface quService();

    public String cacheKey(QueryRequest queryRequest) {
        String hash = GuidService.getXXHash(queryRequest.getCoreQuery().getQuery());
        return String.format(cacheKeyPattern()
                , SysConfigUtil.getAsString("ServiceInstance", "INSTANCE_KEY", INSTANCE_KEY)
                , handlerKey()
                , hash
        );
    }

    @Override
    protected Map<String, Object> doBuildParam(QueryRequest queryRequest) {
        WorkerCoreQuery coreQuery = queryRequest.getCoreQuery();
        QueryConfig queryConfig = quService().handle(coreQuery);
        queryRequest.setQueryConfig(queryConfig);
        return new HashMap<>();
    }

    @Override
    protected Map<String, Object> doDataQuery(QueryRequest queryRequest, Map<String, Object> param) {
        Map<String, Object> resultMap = operators().values().parallelStream()
                .filter(WorkerSearchOperator::operatorActive)
                .map(operator -> Pair.of(operator.operatorName(), operator.handle(queryRequest)))
                .collect(Collectors.toConcurrentMap(Pair::getKey, Pair::getValue));
        log.info("Built {} groups of results", resultMap.size());
        return resultMap;
    }

    @Override
    protected List<SortItem> doParseResult(QueryRequest queryRequest, Map<String, Object> queryResultMap) {
        int groupSize = queryRequest.getCoreQuery().getGroupSize();

        ConcurrentMap<String, List<DocItem>> docGroups = doBuildDocGroupsMap(queryResultMap);
        log.info("Got {} potential doc", docGroups.size());

        List<SortItem> dataList = doAggDocs(groupSize, docGroups);

        log.info("Built {} data in [{}]", dataList.size(), handlerKey());
        return dataList;
    }

    protected List<SortItem> doAggDocs(int groupSize, ConcurrentMap<String, List<DocItem>> docGroups) {
        return docGroups.entrySet().parallelStream()
                .filter(e -> ExtraCollectionUtils.isNotEmpty(e.getValue()))
                .map(docsConverter(groupSize))
                .sorted(Comparator.comparing(SortItem::getScore).reversed())
                .collect(Collectors.toList());
    }

    protected Function<Map.Entry<String, List<DocItem>>, SortItem> docsConverter(int groupSize) {
        return e -> {
            List<DocItem> baseList = e.getValue();
            AtomicBoolean probableMatch = new AtomicBoolean(false);
            double totalScore = baseList.stream()
                    .peek(docItem -> {
                        if ("doc".equalsIgnoreCase(docItem.getDocType())) {
                            probableMatch.set(true);
                        }
                    })
                    .mapToDouble(DocItem::getFinalScore)
                    .sum();

            List<DocItem> docItems = DataUtils.handlePaging(0, groupSize, baseList);
            return SortItem.builder()
                    .dataType(SysConfigUtil.getAsString("ServiceInstance", "INSTANCE_KEY", INSTANCE_KEY))
                    .probablyMatch(probableMatch.get())
                    .title(docItems.get(0).getTitle())
                    .bundleKey(e.getKey())
                    .dataList(docItems)
                    .score(totalScore)
                    .build();
        };
    }

    protected ConcurrentMap<String, List<DocItem>> doBuildDocGroupsMap(Map<String, Object> queryResultMap) {
        Set<String> distinct = ConcurrentHashMap.newKeySet();
        return queryResultMap.entrySet().parallelStream()
                .filter(e -> e.getValue() instanceof List)
                .map(e -> (List<DocItem>) e.getValue())
                .filter(ExtraCollectionUtils::isNotEmpty)
                .flatMap(Collection::stream)
                .filter(docItem -> distinct.add(docItem.getFuncId()))
                .sorted(Comparator.comparing(DocItem::getFinalScore).reversed())
                .collect(Collectors.groupingByConcurrent(DocItem::getBundleKey));
    }

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
    public Map<String, Object> findConfig() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("HANDLER_KEY", HANDLER_KEY);
        config.put("OPERATOR_ACTIVE", OPERATOR_ACTIVE);
        config.put("CONFIDENCE", CONFIDENCE);

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