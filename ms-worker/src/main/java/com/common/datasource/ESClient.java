package com.common.datasource;

import com.alibaba.fastjson.JSON;
import com.common.constants.Constants.ESConfig;
import com.common.constants.Constants.ResultConfig;
import com.common.constants.ResultEnum;
import com.common.utils.DataUtils;
import com.common.utils.ExtraCollectionUtils;
import com.exception.MsWorkerException;
import com.model.config.ESConfigBean;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.ParsedSingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.*;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Data
@Slf4j
@Service
@SuppressWarnings({ "unchecked" })
public class ESClient {

    protected String ES_ADDRESSES;
    protected String ES_USER;
    protected String ES_PWD;

    protected int ES_BULK_SIZE;
    protected int ES_BULK_FLUSH;
    protected int ES_SOCKET_TIMEOUT;
    protected int ES_CONNECT_TIMEOUT;
    protected int ES_BULK_CONCURRENT;
    protected int ES_CONNECTION_REQUEST_TIMEOUT;

    protected RestClient restClient;
    protected BulkProcessor bulkProcessor;
    protected RestHighLevelClient restHighLevelClient;
    protected RequestOptions COMMON_OPTIONS = RequestOptions.DEFAULT.toBuilder().build();

    protected ESClient() {}

    public ESClient(ESConfigBean esConfig) {

        initStaticVariables(esConfig);
        initESClients();
    }

    protected void initStaticVariables(ESConfigBean esConfig) {
        ES_ADDRESSES = Optional.ofNullable(esConfig.getEsAddress()).orElse(ESConfig.DEFAULT_ES_ADDRESS);
        ES_USER = Optional.ofNullable(esConfig.getEsAuthUserName()).orElse("");
        ES_PWD = Optional.ofNullable(esConfig.getEsAddress()).orElse("");

        ES_BULK_SIZE = Optional.of(esConfig.getBulkSize()).orElse(ESConfig.DEFAULT_ES_BULK_SIZE);
        ES_BULK_FLUSH = Optional.of(esConfig.getBulkFlush()).orElse(ESConfig.DEFAULT_ES_BULK_FLUSH);
        ES_SOCKET_TIMEOUT = Optional.of(esConfig.getSocketTimeout()).orElse(ESConfig.DEFAULT_ES_SOCKET_TIMEOUT);
        ES_BULK_CONCURRENT = Optional.of(esConfig.getBulkConcurrent()).orElse(ESConfig.DEFAULT_ES_BULK_CONCURRENT);
        ES_CONNECT_TIMEOUT = Optional.of(esConfig.getConnectTimeout()).orElse(ESConfig.DEFAULT_ES_CONNECT_TIMEOUT);
        ES_CONNECTION_REQUEST_TIMEOUT = Optional.of(esConfig.getConnectRequestTimeout()).orElse(ESConfig.DEFAULT_ES_CONNECTION_REQUEST_TIMEOUT);
    }

    public ESConfigBean getESConfig() {

        return ESConfigBean.builder()
                .esAddress(ES_ADDRESSES)
                .esAuthUserName(ES_USER)
                .bulkSize(ES_BULK_SIZE)
                .bulkFlush(ES_BULK_FLUSH)
                .socketTimeout(ES_SOCKET_TIMEOUT)
                .bulkConcurrent(ES_BULK_CONCURRENT)
                .connectTimeout(ES_CONNECT_TIMEOUT)
                .connectRequestTimeout(ES_CONNECTION_REQUEST_TIMEOUT)
                .build();
    }

    protected void initESClients() {
        String initLog = String.format("Init ES client for address:[%s] %s", ES_ADDRESSES
                , StringUtils.isBlank(ES_USER) ? "" : "by account:[" + ES_USER + "]");
        log.info(initLog);

        try {
            HttpHost[] httpHosts = Arrays.stream(ES_ADDRESSES.split(",")).map(HttpHost::create).toArray(HttpHost[]::new);

            // init address
            RestClientBuilder builder = RestClient.builder(httpHosts);

            // init client base settings
            builder.setRequestConfigCallback((RequestConfig.Builder requestConfigBuilder) ->
                            requestConfigBuilder.setConnectTimeout(ES_CONNECT_TIMEOUT)
                                    .setSocketTimeout(ES_SOCKET_TIMEOUT)
                                    .setConnectionRequestTimeout(ES_CONNECTION_REQUEST_TIMEOUT));

            // if auth, set the base auth settings
            if (StringUtils.isNoneBlank(ES_USER, ES_PWD)) {
                final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(ES_USER, ES_PWD));

                builder.setHttpClientConfigCallback(httpClientBuilder ->
                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
            }

            restHighLevelClient = new RestHighLevelClient(builder);

            bulkProcessor = BulkProcessor
                    .builder(bulkProcessorBuilder(), getBPListener())
                    .setBulkActions(ES_BULK_FLUSH)
                    .setBulkSize(new ByteSizeValue(ES_BULK_SIZE, ByteSizeUnit.MB))
                    .setFlushInterval(TimeValue.timeValueSeconds(10L))
                    .setConcurrentRequests(ES_BULK_CONCURRENT)
                    .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 3))
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            String errMsg = "Error happened when we init ES transport client" + e;
            log.error(errMsg);
            throw new MsWorkerException(ResultEnum.ES_CLIENT_INIT);
        }
    }

    protected BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkProcessorBuilder() {
        return (request, bulkListener) -> {
            try {
                restHighLevelClient.bulkAsync(request, COMMON_OPTIONS, bulkListener);
            } catch (IllegalStateException e) {
                e.printStackTrace();
                log.error("Error happened during we async bulk handle info");
                try {
                    this.destroy();

                    this.initESClients();
                } catch (MsWorkerException e1) {
                    e1.printStackTrace();
                    log.error("Re init client failure");
                    throw e;
                }
            }
        };
    }

    public boolean indexExists(String index) {
        if (StringUtils.isBlank(index)) {
            return false;
        }

        try {
            GetIndexRequest request = new GetIndexRequest(index);
            return restHighLevelClient.indices().exists(request, COMMON_OPTIONS);
        } catch (Exception e) {
            log.error("Error happened when we try to check if index:[{}] exists in ES, {}, retry", index, e);
            throw new MsWorkerException(ResultEnum.ES_CALL, String.format("%s, %s", e.getMessage(), e.getCause()));
        }
    }

    public Map<String, Object> complexSearch(Map<String, Object> param) {

        SearchSourceBuilder sourceBuilder = buildCommonQueryBuilder(param);

        String collapse = DataUtils.getNotNullValue(param, ESConfig.COLLAPSE_KEY, String.class, "");
        if (StringUtils.isNotBlank(collapse)) {
            sourceBuilder.collapse(new CollapseBuilder(collapse));
        }

        // build aggregation
        Map<String, Map<String, Object>> aggregationInfo = DataUtils.getNotNullValue(param, ESConfig.AGGREGATION_KEY, Map.class, new HashMap<>());
        if (!aggregationInfo.isEmpty()) {
            aggregationInfo.entrySet().parallelStream().map(Map.Entry::getValue).map(this::buildCommonAgg).forEach(sourceBuilder::aggregation);
        }

        // if highlight exists
        List<String> highlightList = DataUtils.getNotNullValue(param, ESConfig.HIGHLIGHT_KEY, List.class, new ArrayList<>());
        if (!highlightList.isEmpty()) {
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            highlightList.parallelStream().forEach(highlightBuilder::field);
            sourceBuilder.highlighter(highlightBuilder);
        }

        Map<String, Object> fetchSource = DataUtils.getNotNullValue(param, ESConfig.FETCH_SOURCE_KEY, Map.class, new HashMap<>());
        if (!fetchSource.isEmpty()) {
            List<String> includeList = DataUtils.getNotNullValue(fetchSource, ESConfig.INCLUDES_KEY, List.class, new ArrayList<>());
            List<String> excludeList = DataUtils.getNotNullValue(fetchSource, ESConfig.EXCLUDES_KEY, List.class, new ArrayList<>());
            sourceBuilder.fetchSource(includeList.parallelStream().toArray(String[]::new), excludeList.parallelStream().toArray(String[]::new));
        }

        List<Map<String, Object>> sortConfigs = DataUtils.getNotNullValue(param, ESConfig.SORT_KEY, List.class, new ArrayList<>());
        if (ExtraCollectionUtils.isNotEmpty(sortConfigs)) {
            doBuildSorts(sourceBuilder, sortConfigs);
        }

        Map<String, Object> postFilter = DataUtils.getNotNullValue(param, ESConfig.POST_FILTER_KEY, Map.class, new HashMap<>());
        if (ExtraCollectionUtils.isNotEmpty(postFilter)) {
            sourceBuilder.postFilter(buildBoolQuery(postFilter));
        }

        return fullSearch(param, sourceBuilder);
    }

    public Long dataCount(Map<String, Object> param) {
        SearchSourceBuilder sourceBuilder = buildCommonQueryBuilder(param);

        return doDataCount(param, sourceBuilder);
    }

    protected Long doDataCount(Map<String, Object> param, SearchSourceBuilder sourceBuilder) {

        String trgtIndex = DataUtils.getNotNullValue(param, ESConfig.INDEX_KEY, String.class, "");
        if (!indexExists(trgtIndex)) {
            log.error("No index:[{}] found", trgtIndex);
            return 0L;
        }

        CountRequest request = new CountRequest().indices(trgtIndex).source(sourceBuilder);
        try {
            log.info("Try to query [{}] at:[{}] with request:[{}]", trgtIndex, System.currentTimeMillis(), request.source().toString());
            CountResponse response = restHighLevelClient.count(request, COMMON_OPTIONS);
            long count = response.getCount();
            log.info("Found {} data in index:[{}] by query:[{}]", count,trgtIndex, request.source().toString());
            return count;
        } catch (Exception e) {
            e.printStackTrace();
            String errMsg = String.format("Error happened when we try to query ES by:[%s], %s", request.source().toString(), e);
            log.error(errMsg);
            throw new MsWorkerException(ResultEnum.ES_QUERY, errMsg);
        }
    }

    protected SearchSourceBuilder buildCommonQueryBuilder(Map<String, Object> param) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        // if scroll id exists, no need to build query
        String scrollId = DataUtils.getNotNullValue(param, ResultConfig.SCROLL_ID_KEY, String.class, "");
        if (StringUtils.isBlank(scrollId)) {
            boolean isQuery = false;
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

            // build query conditions
            Map<String, Object> query = DataUtils.getNotNullValue(param, ESConfig.QUERY_KEY, Map.class, new HashMap<>());
            if (!query.isEmpty()) {
                boolQueryBuilder.must(buildBoolQuery(query));
                isQuery = true;
            }

            // build filter conditions
            Map<String, Object> filter = DataUtils.getNotNullValue(param, ESConfig.FILTER_KEY, Map.class, new HashMap<>());
            if (!filter.isEmpty()) {
                boolQueryBuilder.filter(buildBoolQuery(filter));
                isQuery = true;
            }

            // set query & filter conditions
            if (isQuery) {
                sourceBuilder.query(boolQueryBuilder);
            }
        }

        return sourceBuilder;
    }

    protected void doBuildSorts(SearchSourceBuilder sourceBuilder, List<Map<String, Object>> sortConfig) {

        sortConfig.forEach(config -> {
            String type = DataUtils.getNotNullValue(config, ESConfig.TYPE_KEY, String.class, "");
            String order = DataUtils.getNotNullValue(config, ESConfig.ORDER_KEY, String.class, "");
            if (ESConfig.FIELD_SORT_TYPE.equalsIgnoreCase(type)) {
                String field = DataUtils.getNotNullValue(config, ESConfig.FIELD_KEY, String.class, "");
                FieldSortBuilder fieldSortBuilder = SortBuilders.fieldSort(field);
                fieldSortBuilder.order(SortOrder.fromString(order));
                sourceBuilder.sort(fieldSortBuilder);
            } else if (ESConfig.SCRIPT_SORT_TYPE.equalsIgnoreCase(type)) {
                String scriptStr = DataUtils.getNotNullValue(config, ESConfig.SCRIPT_SORT_TYPE, String.class, "");
                if (StringUtils.isNotBlank(scriptStr)) {
                    String scriptSortTypeStr = DataUtils.getNotNullValue(config, ESConfig.SCRIPT_SORT_SCRIPT_TYPE, String.class, ESConfig.NUMBER_TYPE);
                    String sortMode = DataUtils.getNotNullValue(config, ESConfig.SORT_MODE, String.class, "");
                    String scriptTypeStr = DataUtils.getNotNullValue(config, ESConfig.SCRIPT_TYPE, String.class, ESConfig.INLINE_TYPE);
                    String scriptLangStr = DataUtils.getNotNullValue(config, ESConfig.SCRIPT_LANG, String.class, ESConfig.PAINLESS_TYPE);
                    Map<String, String> options = DataUtils.getNotNullValue(config, ESConfig.SCRIPT_OPTIONS, Map.class, Collections.emptyMap());
                    Map<String, Object> params = DataUtils.getNotNullValue(config, ESConfig.SCRIPT_PARAMS, Map.class, Collections.emptyMap());
                    ScriptType scriptType = (ESConfig.INLINE_TYPE.equalsIgnoreCase(scriptTypeStr)) ? ScriptType.INLINE : ScriptType.STORED;
                    Script script = new Script(scriptType, scriptLangStr, scriptStr, options, params);
                    ScriptSortBuilder.ScriptSortType scriptSortType = ScriptSortBuilder.ScriptSortType.fromString(scriptSortTypeStr);
                    ScriptSortBuilder scriptSortBuilder = SortBuilders.scriptSort(script, scriptSortType);
                    scriptSortBuilder.order(SortOrder.fromString(order));
                    if (StringUtils.isNotBlank(sortMode)) {
                        scriptSortBuilder.sortMode(SortMode.fromString(sortMode));
                    }
                    sourceBuilder.sort(scriptSortBuilder);
                }
            }
        });
    }

    protected AggregationBuilder buildCommonAgg(Map<String, Object> param) {
        String type = DataUtils.getNotNullValue(param, ESConfig.TYPE_KEY, String.class, "");
        String aggrName = DataUtils.getNotNullValue(param, ESConfig.NAME_KEY, String.class, "");
        String field = DataUtils.getNotNullValue(param, ESConfig.FIELD_KEY, String.class, "");
        switch (type) {
            case ESConfig.COUNT_KEY:
                return AggregationBuilders.count(aggrName).field(field);
            case ESConfig.SUM_KEY:
                return AggregationBuilders.sum(aggrName).field(field);
            case ESConfig.MAX_KEY:
                return AggregationBuilders.max(aggrName).field(field);
            case ESConfig.MIN_KEY:
                return AggregationBuilders.min(aggrName).field(field);
            case ESConfig.AVG_KEY:
                return AggregationBuilders.avg(aggrName).field(field);
            case ESConfig.STATS_KEY:
                return AggregationBuilders.stats(aggrName).field(field);
            case ESConfig.NESTED_KEY:
                List<Map<String, Object>> subAgg = DataUtils.getNotNullValue(param, ESConfig.SUB_AGG_KEY, List.class, new ArrayList<>());
                String path = DataUtils.getNotNullValue(param, ESConfig.PATH_KEY, String.class, "");
                AggregationBuilder aggregationBuilder = AggregationBuilders.nested(aggrName, path);
                subAgg.parallelStream().map(this::buildCommonAgg).forEach(aggregationBuilder::subAggregation);
                return aggregationBuilder;
            case ESConfig.RANGE_KEY:
                String from = DataUtils.getNotNullValue(param, ESConfig.FROM_KEY, String.class, "");
                String to = DataUtils.getNotNullValue(param, ESConfig.TO_KEY, String.class, "");
                return AggregationBuilders.dateRange(field).addRange(from, to);
            case ESConfig.TERMS_KEY:
            default:
                return buildTermsAggregation(param, aggrName, field);
        }
    }

    protected AggregationBuilder buildTermsAggregation(Map<String, Object> param, String aggregationName, String field) {
        TermsAggregationBuilder termsAgg = AggregationBuilders.terms(aggregationName).field(field);
        int size = DataUtils.getNotNullValue(param, ESConfig.SIZE_KEY, Integer.class, 0);
        if (0 < size) {
            termsAgg.size(size);
        }

        int shard_size = DataUtils.getNotNullValue(param, ESConfig.SHARD_SIZE_KEY, Integer.class, 0);
        if (0 < shard_size) {
            termsAgg.shardSize(shard_size);
        }

        long minDocCount = DataUtils.getNotNullValue(param, ESConfig.MIN_DOC_COUNT_KEY, Integer.class, 0).longValue();
        if (0L < minDocCount) {
            termsAgg.minDocCount(minDocCount);
        }

        long shardMinDocCount = DataUtils.getNotNullValue(param, ESConfig.SHARD_MIN_DOC_COUNT_KEY, Integer.class, 0).longValue();
        if (0L < shardMinDocCount) {
            termsAgg.shardMinDocCount(shardMinDocCount);
        }

        termsAgg.missing(DataUtils.getNotNullValue(param, ESConfig.MISSING_KEY, String.class, ""));

        Object includeObj = param.get(ESConfig.INCLUDES_KEY);
        Object excludeObj = param.get(ESConfig.EXCLUDES_KEY);

        if (null != includeObj || null != excludeObj) {
            IncludeExclude includeExclude = null;
            if (includeObj instanceof String || excludeObj instanceof String) {
                includeExclude = new IncludeExclude(Optional.ofNullable(includeObj).orElse("").toString()
                        , Optional.ofNullable(includeObj).orElse("").toString());
            } else if (includeObj instanceof List || excludeObj instanceof List) {
                List includeList = (List) Optional.ofNullable(includeObj).orElse(new ArrayList<>());
                List excludeList = (List) Optional.ofNullable(excludeObj).orElse(new ArrayList<>());
                Object firstItem = null;
                if (!includeList.isEmpty()) {
                    firstItem = includeList.get(0);
                } else if (!excludeList.isEmpty()) {
                    firstItem = excludeList.get(0);
                }

                if (null != firstItem) {
                    if (firstItem instanceof String) {
                        String[] inArr = (String[]) includeList.parallelStream().toArray(String[]::new);
                        String[] exArr = (String[]) excludeList.parallelStream().toArray(String[]::new);
                        includeExclude = new IncludeExclude((includeList.isEmpty()) ? null : inArr, (excludeList.isEmpty()) ? null : exArr);
                    } else if (firstItem instanceof Double) {
                        double[] inArr = new double[includeList.size()];
                        double[] exArr = new double[excludeList.size()];
                        int inIndex = 0;
                        DataUtils.forEach(inIndex, includeList, (index, list) -> inArr[index] = (double) ((List) list).get(index));
                        int exIndex = 0;
                        DataUtils.forEach(exIndex, excludeList, (index, list) -> exArr[index] = (double) ((List) list).get(index));
                        includeExclude = new IncludeExclude((includeList.isEmpty()) ? null : inArr, (excludeList.isEmpty()) ? null : exArr);
                    } else if (firstItem instanceof Long) {
                        long[] inArr = new long[includeList.size()];
                        long[] exArr = new long[excludeList.size()];
                        int inIndex = 0;
                        DataUtils.forEach(inIndex, includeList, (index, list) -> inArr[index] = (long) ((List) list).get(index));
                        int exIndex = 0;
                        DataUtils.forEach(exIndex, excludeList, (index, list) -> exArr[index] = (long) ((List) list).get(index));
                        includeExclude = new IncludeExclude((includeList.isEmpty()) ? null : inArr, (excludeList.isEmpty()) ? null : exArr);
                    }
                }
            }

            if (null != includeExclude) {
                termsAgg.includeExclude(includeExclude);
            }
        }
        return termsAgg;
    }

    protected BoolQueryBuilder buildBoolQuery(Map<String, Object> param) {

        BoolQueryBuilder query = QueryBuilders.boolQuery();
        param.keySet().stream().filter(ESConfig.BOOL_CONDITION_LIST::contains).forEach(key -> {
            List<Map<String, Object>> trgt = DataUtils.getNotNullValue(param, key, List.class, new ArrayList<>());
            Consumer<QueryBuilder> consumer;
            switch (key) {
                case ESConfig.SHOULD_KEY:
                    consumer = query::should;
                    break;
                case ESConfig.MUST_NOT_KEY:
                    consumer = query::mustNot;
                    break;
                case ESConfig.MUST_KEY:
                default:
                    consumer = query::must;
            }

            trgt.stream().map(this::buildCommonQuery).forEach(consumer);
        });
        return query;
    }

    protected QueryBuilder buildCommonQuery(Map<String, Object> param) {

        QueryBuilder builder;
        Double boost = DataUtils.getNotNullValue(param, ESConfig.BOOST_KEY, Double.class, 1D);
        String type = DataUtils.getNotNullValue(param, ESConfig.TYPE_KEY, String.class, "");
        if (ESConfig.SIMPLE_CONDITION_LIST.contains(type)) {
            builder = buildSimpleQuery(param);
        } else if (ESConfig.RANGE_KEY.equalsIgnoreCase(type)) {
            builder = buildRangeQuery(param);
        } else if (ESConfig.MULTIMATCH_KEY.equalsIgnoreCase(type)) {
            builder = buildMultiMatchQuery(param);
        } else if (ESConfig.NESTED_KEY.equalsIgnoreCase(type)) {
            builder = buildNestedQuery(param);
        } else if (ESConfig.BOOL_KEY.equalsIgnoreCase(type)) {
            builder = buildBoolQuery(param);
        } else {
            builder = QueryBuilders.matchAllQuery();
        }

        builder.boost(boost.floatValue());

        return builder;
    }

    protected QueryBuilder buildNestedQuery(Map<String, Object> param) {
        String path = DataUtils.getNotNullValue(param, ESConfig.PATH_KEY, String.class, "");
        Map<String, Object> query = DataUtils.getNotNullValue(param, ESConfig.QUERY_KEY, Map.class, new HashMap<>());
        return QueryBuilders.nestedQuery(path, buildCommonQuery(query), ScoreMode.Avg);
    }

    protected QueryBuilder buildMultiMatchQuery(Map<String, Object> param) {
        Object value = DataUtils.getNotNullValue(param, ESConfig.VALUE_KEY, Object.class, new Object());
        List<String> fieldNames = DataUtils.getNotNullValue(param, ESConfig.FIELDNAMES_KEY, List.class, new ArrayList<>());
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(value);
        fieldNames.parallelStream().map(x -> x.split("\\^"))
                .forEach(x -> multiMatchQueryBuilder.field(x[0], x.length > 1 ? Float.parseFloat(x[1]) : 1F));

        return multiMatchQueryBuilder;
    }

    protected QueryBuilder buildRangeQuery(Map<String, Object> param) {
        Object field = DataUtils.getNotNullValue(param, ESConfig.FIELD_KEY, Object.class, new Object());
        RangeQueryBuilder queryBuilder = QueryBuilders.rangeQuery(String.valueOf(field).trim());
        param.keySet().parallelStream().forEach(key -> {
            Object value = DataUtils.getNotNullValue(param, key, Object.class, new Object());
            switch (key) {
                case ESConfig.INCLUDE_LOWER_KEY:
                    queryBuilder.includeLower((boolean) value);
                    break;
                case ESConfig.INCLUDE_UPPER_KEY:
                    queryBuilder.includeUpper((boolean) value);
                    break;
                case ESConfig.FROM_KEY:
                    queryBuilder.from(value);
                    break;
                case ESConfig.LTE_KEY:
                    queryBuilder.lte(value);
                    break;
                case ESConfig.GTE_KEY:
                    queryBuilder.gte(value);
                    break;
                case ESConfig.LT_KEY:
                    queryBuilder.lt(value);
                    break;
                case ESConfig.GT_KEY:
                    queryBuilder.gt(value);
                    break;
                case ESConfig.TO_KEY:
                    queryBuilder.to(value);
                    break;
                default:
                    break;
            }
        });
        return queryBuilder;
    }

    protected QueryBuilder buildSimpleQuery(Map<String, Object> param) {

        String type = DataUtils.getNotNullValue(param, ESConfig.TYPE_KEY, String.class, "");
        Object field = DataUtils.getNotNullValue(param, ESConfig.FIELD_KEY, Object.class, new Object());
        Object value = DataUtils.getNotNullValue(param, ESConfig.VALUE_KEY, Object.class, new Object());
        switch (type) {
            case ESConfig.EXISTS_KEY:
                return QueryBuilders.existsQuery(String.valueOf(field).trim());
            case ESConfig.MATCH_KEY:
                return QueryBuilders.matchQuery(String.valueOf(field).trim(), value);
            case ESConfig.TERM_KEY:
                return QueryBuilders.termQuery(String.valueOf(field).trim(), value);
            case ESConfig.FUZZY_KEY:
                return QueryBuilders.fuzzyQuery(String.valueOf(field).trim(), value);
            case ESConfig.PREFIX_KEY:
                return QueryBuilders.prefixQuery(String.valueOf(field).trim(), String.valueOf(value).trim());
            case ESConfig.REGEXP_KEY:
                return QueryBuilders.regexpQuery(String.valueOf(field).trim(), String.valueOf(value).trim());
            case ESConfig.WRAPPER_KEY:
                return QueryBuilders.wrapperQuery(String.valueOf(value).trim());
            case ESConfig.WILDCARD_KEY:
                return QueryBuilders.wildcardQuery(String.valueOf(field).trim(), String.valueOf(value).trim());
            case ESConfig.QUERY_STRING_KEY:
                return QueryBuilders.queryStringQuery(String.valueOf(value).trim());
            case ESConfig.MATCH_PHRASE_KEY:
                return QueryBuilders.matchPhraseQuery(String.valueOf(field).trim(), value);
            case ESConfig.MATCH_PHRASE_PREFIX_KEY:
                return QueryBuilders.matchPhrasePrefixQuery(String.valueOf(field).trim(), value);
            default:
                return QueryBuilders.termsQuery(String.valueOf(field).trim(), (Collection<?>) value);
        }
    }

    protected void makeBaseSearchBuilder(Map<String, Object> param, SearchSourceBuilder sourceBuilder) {
        int from = DataUtils.getNotNullValue(param, ESConfig.FROM_KEY, Integer.class, ESConfig.DEFAULT_ES_FROM);
        int size = DataUtils.getNotNullValue(param, ESConfig.SIZE_KEY, Integer.class, ESConfig.DEFAULT_ES_SIZE);
        if (from + size > ESConfig.DEFAULT_ES_MAX_SIZE) {
            log.error("Over size limit, please try scroll");
            size = ESConfig.DEFAULT_ES_MAX_SIZE - from;
        }

        sourceBuilder
                .from(from)
                .size(size)
//                .trackTotalHits(true)
        ;
    }

    protected Map<String, Object> fullSearch(Map<String, Object> param, SearchSourceBuilder sourceBuilder) {
        String trgtIndex = DataUtils.getNotNullValue(param, ESConfig.INDEX_KEY, String.class, "");
        makeBaseSearchBuilder(param, sourceBuilder);
        SearchRequest request = new SearchRequest()
                .indices(trgtIndex)
                .source(sourceBuilder);

        if (!indexExists(trgtIndex)) {
            log.warn("No index found:[{}]", trgtIndex);
            return getBaseResult();
        }

        try {
            Long startTime = System.nanoTime();
            SearchResponse response = doQuery(param, request);
            log.debug("Got response as:[{}]", response);
            Long endTime = System.nanoTime();
            log.info("Finish query at:[{}] which took:[{}]", endTime, (endTime - startTime) / 1_000_000);
            return buildResult(response);
        } catch (Exception e) {
            e.printStackTrace();
            String errMsg = String.format("Error happened when we try to query ES by:[%s], %s", request.source().toString(), e);
            log.error(errMsg);
            throw new MsWorkerException(ResultEnum.ES_QUERY, errMsg);
        }
    }

    protected SearchResponse doQuery(Map<String, Object> param, SearchRequest request) throws IOException {
        String scrollId = DataUtils.getNotNullValue(param, ResultConfig.SCROLL_ID_KEY, String.class, "");
        String timeValue = DataUtils.getNotNullValue(param, ESConfig.SCROLL_TIME_VALUE_KEY, String.class, "");
        if (StringUtils.isNotBlank(timeValue)) {
            TimeValue tv = TimeValue.parseTimeValue(timeValue, ESConfig.SCROLL_TIME_VALUE_KEY);
            if (StringUtils.isNotBlank(scrollId)) {
                return restHighLevelClient.scroll(new SearchScrollRequest(scrollId).scroll(tv), COMMON_OPTIONS);
            } else {
                request.scroll(tv);
            }
        }

        String trgtIndex = DataUtils.getNotNullValue(param, ESConfig.INDEX_KEY, String.class, "");
        log.info("Try to query [{}] at:[{}] with request:[{}]", trgtIndex, System.currentTimeMillis(), request.source().toString());
        return restHighLevelClient.search(request, COMMON_OPTIONS);
    }

    protected Map<String, Object> buildResult(SearchResponse response) {
        SearchHits hits = response.getHits();
        long total = hits.getTotalHits().value;
        log.debug("Got {} data in total", total);
        Map<String, Object> result = getBaseResult();
        result.put(ResultConfig.TOTAL_KEY, total);

        List<Map<String, Object>> dataList = Stream.of(hits.getHits()).map(x -> {
            Map<String, Object> sourceAsMap = x.getSourceAsMap();
            sourceAsMap.put(ESConfig.SCORE_KEY, x.getScore());
            sourceAsMap.put(ESConfig.ES_ID, x.getId());
            sourceAsMap.put(ESConfig.ES_INDEX, x.getIndex());

            Map<String, HighlightField> highlightFields = x.getHighlightFields();
            if (ExtraCollectionUtils.isNotEmpty(highlightFields)) {
                Map<String, Object> highlight = highlightFields.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().fragments()[0].string()));
                sourceAsMap.put(ResultConfig.HIGHLIGHT_KEY, highlight);
            }
            return sourceAsMap;
        }).collect(Collectors.toList());

        result.put(ResultConfig.DATA_KEY, dataList);
        log.debug("Build as {} data", dataList.size());

        Map<String, Object> aggMap = new HashMap<>();
        List<Aggregation> aggregations = Optional.ofNullable(response.getAggregations())
                .orElse(new Aggregations(Collections.emptyList())).asList();
        aggregations.parallelStream().forEach(aggregation -> getAggrInfo(aggMap, "", aggregation));
        log.debug("Build as {} aggregation data", aggMap.size());
        result.put(ResultConfig.AGGREGATION_KEY, aggMap);

        String scrollId = response.getScrollId();
        if (StringUtils.isNotBlank(scrollId)) {
            log.debug("Generated scroll id:[{}]", scrollId);
            result.put(ResultConfig.SCROLL_ID_KEY, scrollId);
        }

        return result;
    }

    protected void getAggrInfo(Map<String, Object> aggMap, String parentName, Aggregation aggregation) {

        String key = (StringUtils.isNotBlank(parentName)) ? parentName + "." + aggregation.getName() : aggregation.getName();
        if (aggregation instanceof ParsedSingleBucketAggregation) {
            ParsedSingleBucketAggregation parsedNested = (ParsedSingleBucketAggregation) aggregation;
            aggMap.put(key + ".count", Long.toString(parsedNested.getDocCount()));
            List<Aggregation> aggregations = Optional.ofNullable(parsedNested.getAggregations()).orElse(new Aggregations(Collections.emptyList())).asList();
            aggregations.forEach(subAggregation -> getAggrInfo(aggMap, key, subAggregation));
        } else if (aggregation instanceof NumericMetricsAggregation.SingleValue) {
            NumericMetricsAggregation.SingleValue singleValue = (NumericMetricsAggregation.SingleValue) aggregation;
            aggMap.put(key + ".value", singleValue.getValueAsString());
        } else if (aggregation instanceof MultiBucketsAggregation) {
            MultiBucketsAggregation multiBucketsAggregation = (MultiBucketsAggregation) aggregation;
            List<Map<String, Object>> bucketList = multiBucketsAggregation.getBuckets().parallelStream()
                    .map(x -> {
                                Map<String, Object> map = new HashMap<>();
                                map.put("key", x.getKey());
                                map.put("count", x.getDocCount());
                                return map;
                            }
                    ).collect(Collectors.toList());
            aggMap.put(key + ".value", bucketList);
        }
    }

    public Integer bulkInsert(String index, String idKey, Map<String, ?> data) {

        String pk = String.valueOf(DataUtils.getNotNullValue(data, idKey, Object.class, "")).trim();
        IndexRequest indexRequest = new IndexRequest(index).source(data);
        if (StringUtils.isNotBlank(pk)) {
            indexRequest.id(pk);
        }

        bulkProcessor.add(indexRequest);
        return 1;
    }

    protected Map<String, Object> getBaseResult() {
        Map<String, Object> baseResult = new HashMap<>();
        baseResult.put(ResultConfig.TOTAL_KEY, 0);
        baseResult.put(ResultConfig.DATA_KEY, new ArrayList<>());
        baseResult.put(ResultConfig.AGGREGATION_KEY, new HashMap<>());
        return baseResult;
    }

    protected BulkProcessor.Listener getBPListener() {
        return new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                log.info("Start to handle bulk commit executionId:[{}] for {} requests", executionId, request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                log.info("Finished handling bulk commit executionId:[{}] for {} requests", executionId, request.numberOfActions());

                if (response.hasFailures()) {
                    AtomicInteger count = new AtomicInteger();
                    response.spliterator().forEachRemaining(x -> {
                        if (x.isFailed()) {
                            BulkItemResponse.Failure failure = x.getFailure();
                            String msg = String.format(
                                    "Index:[%s], type:[%s], id:[%s], itemId:[%s], opt:[%s], version:[%s], errMsg:%s"
                                    , x.getIndex()
                                    , x.getType()
                                    , x.getId()
                                    , x.getItemId()
                                    , x.getOpType().getLowercase()
                                    , x.getVersion()
                                    , failure.getCause().getMessage()
                            );
                            log.error("Bulk executionId:[{}] has error messages:\t{}", executionId, msg);
                            count.incrementAndGet();
                        }
                    });
                    log.info("Finished handling bulk commit executionId:[{}] for {} requests with {} errors", executionId, request.numberOfActions(), count.intValue());
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                failure.printStackTrace();
                Class clazz = failure.getClass();
                log.error("Bulk [{}] finished with [{}] requests of error:{}, {}, {}:-[{}], {}", executionId
                        , request.numberOfActions()
                        , clazz.getName()
                        , clazz.getSimpleName()
                        , clazz.getTypeName()
                        , clazz.getCanonicalName()
                        ,failure.getMessage());
                request.requests().stream().filter(x -> x instanceof IndexRequest)
                        .forEach(x -> {
                            Map<String, Object> source = ((IndexRequest) x).sourceAsMap();
                            String pk = DataUtils.getNotNullValue(source, "id", String.class, "");
                            log.error("Failure to handle index:[{}], type:[{}] id:[{}], data:[{}]", x.index(), x.type(), pk, JSON.toJSONString(source));
                        });

                if (failure instanceof IllegalStateException) {
                    synchronized (ESClient.class) {
                        try {
                            initESClients();
                        } catch (MsWorkerException e) {
                            e.printStackTrace();
                            log.error("Re init ES client failure");
                        }
                    }
                }
            }
        };
    }

    @PreDestroy
    public void destroy() {
        try {
            if (null != bulkProcessor) {
                boolean terminated = bulkProcessor.awaitClose(30L, TimeUnit.SECONDS);
                if (terminated) {
                    if (null != restClient) {
                        restClient.close();
                    }

                    if (null != restHighLevelClient) {
                        restHighLevelClient.close();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            String errMsg = "Error happened when we try to close ES client" + e;
            log.error(errMsg);
            throw new MsWorkerException(ResultEnum.ES_CLIENT_CLOSE);
        }
    }
}
