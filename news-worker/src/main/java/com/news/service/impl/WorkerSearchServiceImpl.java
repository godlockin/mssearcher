package com.news.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONValidator;
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

    @Value("${SEARCH_WORKER_REMOTE_DETAILS_QUERY_URL:}")
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
                .map(DocItem::getBundleKey)
                .collect(Collectors.toSet());
        if (ExtraCollectionUtils.isEmpty(docIds)) {
            log.warn("No doc found");
            return new ArrayList<>();
        }

        // call remote service for docs' details
        Map<String, DocItem> docMap = findDocDetails(new ArrayList<>(docIds));

        // group by the docs' content, only 1 content got for each news till now
        List<SortItem> dataList = doAggDocs(groupSize, docGroups, docMap);

        log.info("Built {} data in [{}]", dataList.size(), handlerKey());
        return dataList;
    }

    /**
     * do find all the doc
     * @param idList for all docs
     * @return docs' mapping, id -> doc info
     */
    private Map<String, DocItem> findDocDetails(List<String> idList) {
        Map<String, Object> query = new HashMap<>();
        query.put("queryType", "LIST");
        Map<String, Object> params = new HashMap<>();
        params.put("uuid", idList);
        query.put("params", params);

        String url = SysConfigUtil.getAsString(handlerKey(), "REMOTE_DETAILS_QUERY_URL", REMOTE_DETAILS_QUERY_URL);
        String resultStr = RestHttpClient.doPost(url, query);
        Optional<List<JSONObject>> optionalList = Optional.ofNullable(resultStr)
                .filter(StringUtils::isNotBlank)
                .filter(str -> JSONValidator.from(str).validate())
                .map(JSONObject::parseObject)
                .filter(json -> 200 == (Integer) json.getOrDefault("code", -1))
                .filter(json -> ExtraCollectionUtils.isNotEmpty(json.getJSONObject("data")))
                .map(json -> json.getJSONObject("data"))
                .filter(json -> ExtraCollectionUtils.isNotEmpty((List) json.getOrDefault("list", new ArrayList<>())))
                .map(json -> json.getJSONArray("list"))
                .map(jsonArray -> jsonArray
                        .stream()
                        .filter(o -> o instanceof JSONObject)
                        .map(o -> (JSONObject) o)
                        .filter(ExtraCollectionUtils::isNotEmpty)
                        .collect(Collectors.toList()));
        if (!optionalList.isPresent()) {
            log.error("No data found from:[{}] for ids:[{}]", url, idList);
            return new HashMap<>();
        }

        ConcurrentHashMap<String, DocItem> map = new ConcurrentHashMap<>();
        List<JSONObject> dataList = optionalList.get();
        dataList.parallelStream().map(json -> {
            DocItem docItem = new DocItem();

            JSONObject basic = (JSONObject) json.getOrDefault("basic", new JSONObject());
            JSONObject content = basic.getJSONObject("content");
            JSONObject sourceFrom = basic.getJSONObject("sourceFrom");
            if (ExtraCollectionUtils.isAnyEmpty(basic, content, sourceFrom)) {
                return docItem;
            }

            docItem.setBundleKey(basic.getString("uuid"));
            docItem.setFuncId(json.get("id").toString());

            docItem.setTitle(content.getString("headline"));
            String summary = content.getString("summary");
            if (StringUtils.isNotBlank(summary)) {
                docItem.setSummary(summary);
            } else {
                String contentStr = content.getString("content");
                if (StringUtils.isNotBlank(contentStr)) {
                    docItem.setSummary(contentStr.length() > 200 ? contentStr.substring(0, 200) : contentStr);
                } else {
                    docItem.setSummary((String) content.getOrDefault("subheading", ""));
                }
            }

            docItem.setSourceUrl(sourceFrom.getString("originUrl"));

            JSONObject subSourceFrom = sourceFrom.getJSONObject("sourceFrom");
            if (ExtraCollectionUtils.isNotEmpty(subSourceFrom)) {
                docItem.setDomain(subSourceFrom.getString("name"));
            }

            JSONArray pictureList = basic.getJSONArray("pictureList");
            if (ExtraCollectionUtils.isNotEmpty(pictureList)) {
                JSONObject pic = pictureList.getJSONObject(0);
                docItem.setHeadImageUrl(pic.getString("ossUrl"));
            }

            return docItem;
        }).filter(docItem -> StringUtils.isNoneBlank(
                docItem.getBundleKey()
                , docItem.getTitle()
        ))
        .forEach(docItem -> map.put(docItem.getBundleKey(), docItem));

        log.info("Got {} docInfo for ids:[{}]", map.size(), idList);
        return new HashMap<>(map);
    }

    protected Function<Map.Entry<String, List<DocItem>>, SortItem> docsConverter(
            int groupSize, Map<String, DocItem> docMap) {
        return e -> {
            List<DocItem> baseList = e.getValue();
            List<DocItem> tmpList = Collections.synchronizedList(new ArrayList<>());
            AtomicBoolean probableMatch = new AtomicBoolean(false);
            double totalScore = baseList.stream()
                    .peek(docItem -> {
                        if ("doc".equalsIgnoreCase(docItem.getDocType())) {
                            probableMatch.set(true);
                        }

                        String funcId = docItem.getBundleKey();
                        DocItem data = docMap.getOrDefault(funcId, new DocItem(docItem));
                        tmpList.add(data);
                    })
                    .mapToDouble(DocItem::getFinalScore)
                    .sum();

            List<DocItem> docItems = DataUtils.handlePaging(0, groupSize, tmpList);
            return SortItem.builder()
                    .dataType(SysConfigUtil.getAsString("ServiceInstance", "INSTANCE_KEY", INSTANCE_KEY))
                    .title(ExtraCollectionUtils.isNotEmpty(docItems) ? docItems.get(0).getTitle() : "")
                    .probablyMatch(probableMatch.get())
                    .bundleKey(e.getKey())
                    .dataList(docItems)
                    .score(totalScore)
                    .build();
        };
    }

    protected List<SortItem> doAggDocs(
            int groupSize, ConcurrentMap<String, List<DocItem>> docGroups, Map<String, DocItem> docMap) {
        Set<String> distinct = ConcurrentHashMap.newKeySet();
        return docGroups.entrySet().parallelStream()
                .filter(e -> ExtraCollectionUtils.isNotEmpty(e.getValue()))
                .map(docsConverter(groupSize, docMap))
                .filter(sortItem -> StringUtils.isNoneBlank(sortItem.getBundleKey(), sortItem.getTitle()))
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
    protected Map<String, WorkerSearchOperator> operators() {
        return this.operators;
    }

    @Override
    protected WorkerQuServiceInterface quService() {
        return this.quService;
    }

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