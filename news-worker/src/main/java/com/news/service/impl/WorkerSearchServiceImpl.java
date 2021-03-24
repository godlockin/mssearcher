package com.news.service.impl;

import com.common.SysConfigUtil;
import com.common.utils.DataUtils;
import com.common.utils.ExtraCollectionUtils;
import com.common.utils.RestHttpClient;
import com.github.benmanes.caffeine.cache.Cache;
import com.model.DocItem;
import com.model.SortItem;
import com.model.input.QueryRequest;
import com.service.WorkerQuServiceInterface;
import com.service.WorkerSearchServiceInterface;
import com.service.worker.AbstractWorkerSearchService;
import com.service.worker.operators.WorkerSearchOperator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
public class WorkerSearchServiceImpl extends AbstractWorkerSearchService implements WorkerSearchServiceInterface {

    @Value("${SEARCH_INSTANCE_KEY:NEWS_SEARCH_WORKER}")
    private String INSTANCE_KEY;

    @Value("${SEARCH_WORKER_SEARCH_HANDLER_KEY:NEWS_SEARCH}")
    private String HANDLER_KEY;

    @Value("${SEARCH_WORKER_SEARCH_TIMEOUT:2000}")
    private int TIMEOUT;

    @Value("${SEARCH_WORKER_REMOTE_DETAILS_QUERY_URL:NEWS_SEARCH}")
    private String REMOTE_DETAILS_QUERY_URL;

    private Cache<String, List<DocItem>> localCache;
    private Cache<String, List<DocItem>> redisCache;

    @Autowired
    private Map<String, WorkerSearchOperator> operators;

    @Autowired
    private WorkerQuServiceInterface quService;

    @Override
    protected List<SortItem> doParseResult(QueryRequest queryRequest, Map<String, Object> queryResultMap) {
        int groupSize = queryRequest.getCoreQuery().getGroupSize();

        ConcurrentMap<String, List<DocItem>> docGroups = doBuildDocGroupsMap(queryResultMap);
        log.info("Got {} potential doc", docGroups.size());

        Set<String> docIds = docGroups.entrySet().parallelStream()
                .filter(e -> ExtraCollectionUtils.isNotEmpty(e.getValue()))
                .map(Map.Entry::getValue)
                .flatMap(Collection::parallelStream)
                .map(DocItem::getFuncId)
                .collect(Collectors.toSet());
        if (ExtraCollectionUtils.isEmpty(docIds)) {
            log.warn("No doc found");
            return new ArrayList<>();
        }

        Map<String, Object> docMap = findDocDetails(new ArrayList<>(docIds));

        List<SortItem> dataList = doAggDocs(groupSize, docGroups, docMap);

        log.info("Built {} data in [{}]", dataList.size(), handlerKey());
        return dataList;
    }

    private Map<String, Object> findDocDetails(List<String> idList) {
        Map<String, Object> query = new HashMap<>();
        query.put("queryType", "LIST");
        Map<String, Object> params = new HashMap<>();
        params.put("id", idList);
        query.put("params", params);

        String url = SysConfigUtil.getAsString(handlerKey(), "REMOTE_DETAILS_QUERY_URL", REMOTE_DETAILS_QUERY_URL);
        String resultStr = RestHttpClient.doPost(url, query);

        return new HashMap<>();
    }

    protected Function<Map.Entry<String, List<DocItem>>, SortItem> docsConverter(int groupSize, Map<String, Object> docMap) {
        return e -> {
            List<DocItem> baseList = e.getValue();
            AtomicBoolean probableMatch = new AtomicBoolean(false);
            double totalScore = baseList.stream()
                    .peek(docItem -> {
                        if ("doc".equalsIgnoreCase(docItem.getDocType())) {
                            probableMatch.set(true);
                        }
                        String funcId = docItem.getFuncId();
                        Object data = docMap.get(funcId);
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

    protected List<SortItem> doAggDocs(int groupSize, ConcurrentMap<String, List<DocItem>> docGroups, Map<String, Object> docMap) {
        Set<String> distinct = ConcurrentHashMap.newKeySet();
        return docGroups.entrySet().parallelStream()
                .filter(e -> ExtraCollectionUtils.isNotEmpty(e.getValue()))
                .map(docsConverter(groupSize, docMap))
                .sorted(Comparator.comparing(SortItem::getScore).reversed())
                .filter(item -> {
                    String title = item.getTitle().replaceAll("[^a-zA-Z0-9\u4E00-\u9FA5]", "");
                    title = DataUtils.removeUselessWhiteSpace(title);
                    return StringUtils.isNotBlank(title) && distinct.add(title);
                })
                .collect(Collectors.toList());
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
    protected Map<String, WorkerSearchOperator> operators() { return this.operators; }

    @Override
    protected WorkerQuServiceInterface quService() { return this.quService; }

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