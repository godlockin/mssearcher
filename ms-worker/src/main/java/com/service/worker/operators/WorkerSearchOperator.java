package com.service.worker.operators;

import com.alibaba.fastjson.JSON;
import com.common.ESClientUtil;
import com.common.SysConfigUtil;
import com.common.constants.Constants.ESConfig;
import com.common.constants.Constants.ResultConfig;
import com.common.constants.DecayTypeEnum;
import com.common.datasource.ESClient;
import com.common.utils.DateUtils;
import com.model.DocItem;
import com.model.config.ESConfigBean;
import com.model.input.QueryRequest;
import com.service.worker.AbstractWorkerCacheAbleService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

@Slf4j
@Service
public abstract class WorkerSearchOperator extends AbstractWorkerCacheAbleService<QueryRequest, List<DocItem>> {

    protected ESConfigBean buildESConfigBean() {
        return ESConfigBean.builder()
                .esAddress(SysConfigUtil.getAsString(operatorName(), "ES_ADDRESS", ESConfig.DEFAULT_ES_ADDRESS))
                .esAuthUserName(SysConfigUtil.getAsString(operatorName(), "ES_USERNAME", ESConfig.DEFAULT_ES_USERNAME))
                .esAuthPassword(SysConfigUtil.getAsString(operatorName(), "ES_PASSWORD", ESConfig.DEFAULT_ES_PASSWORD))
                .bulkSize(SysConfigUtil.getAsInteger(operatorName(), "ES_BULK_SIZE", ESConfig.DEFAULT_ES_BULK_SIZE))
                .bulkFlush(SysConfigUtil.getAsInteger(operatorName(), "ES_BULK_FLUSH", ESConfig.DEFAULT_ES_BULK_FLUSH))
                .socketTimeout(SysConfigUtil.getAsInteger(operatorName(), "ES_SOCKET_TIMEOUT", ESConfig.DEFAULT_ES_SOCKET_TIMEOUT))
                .bulkConcurrent(SysConfigUtil.getAsInteger(operatorName(), "ES_BULK_CONCURRENT", ESConfig.DEFAULT_ES_BULK_CONCURRENT))
                .connectTimeout(SysConfigUtil.getAsInteger(operatorName(), "ES_CONNECT_TIMEOUT", ESConfig.DEFAULT_ES_CONNECT_TIMEOUT))
                .connectRequestTimeout(SysConfigUtil.getAsInteger(operatorName(), "ES_CONNECTION_REQUEST_TIMEOUT", ESConfig.DEFAULT_ES_CONNECTION_REQUEST_TIMEOUT))
                .build();
    }

    protected ESClient buildESClient() { return ESClientUtil.initClient(handlerKey(), buildESConfigBean()); }

    protected abstract ESClient esClient();

    protected abstract String esIndex();

    protected Map<String, Object> buildConditionItem(String type, String field, Object value, Double boost) {
        Map<String, Object> param = new HashMap<>();
        param.put(ESConfig.TYPE_KEY, type);
        param.put(ESConfig.FIELD_KEY, field);
        param.put(ESConfig.VALUE_KEY, value);
        param.put(ESConfig.BOOST_KEY, boost);
        return param;
    }

    protected double scoreDecay(DocItem docItem) {
        long nowMillTime = DateUtils.getZonedDateTime().getLong(ChronoField.MILLI_OF_SECOND);

        double finalScore = decayFunc().apply(nowMillTime, docItem);
        log.debug("[{}] do decay on doc:[{}], original score:[{}], final score:[{}]"
                , handlerKey()
                , docItem.getBundleKey()
                , docItem.getOriScore()
                , finalScore
        );
        return finalScore;
    }

    protected BiFunction<Long, DocItem, Double> decayFunc() {
        DecayTypeEnum decayTypeEnum = DecayTypeEnum.parse(decayType());
        switch (decayTypeEnum) {
            case SIGMOID:
                return sigmoidDecay();

            case HYPERBOLIC:
                return hyperbolicDecay();

            case UNIFORM:
                return uniformDecay();

            case NONE:
            default:
                return defaultDecay();
        }
    }

    protected abstract String decayType();

    protected abstract double workerTimeDecayRate();

    //sigmoid，适用于研报文本
    private BiFunction<Long, DocItem, Double> sigmoidDecay() {
        return (seed, docItem) -> {
            ZonedDateTime zonedDateTime = DateUtils.getLegalDate(docItem.getPublishDate());
            long publishMillTime = Timestamp.from(zonedDateTime.toInstant()).getTime();
            long hourDiff = (seed - publishMillTime) / (1_000 * 3_600);
            double quarterDiff = hourDiff * 1.0 / (24 * 90);
            double workerTimeDecay = workerTimeDecayRate();
            double decayWeight = workerTimeDecay / (Math.exp(quarterDiff - 4) + 1) + (1 - workerTimeDecay);
            double oriScore = docItem.getOriScore();
            double finalScore = oriScore * decayWeight;
            log.debug("Use sigmoid decay on [{}], got weight:[{}], original score:[{}] final score:[{}], original data:[{}]"
                    , handlerKey()
                    , decayWeight
                    , oriScore
                    , finalScore
                    , JSON.toJSONString(docItem)
            );
            return finalScore;
        };
    }

    //双曲线, 适用于公告或新闻
    public BiFunction<Long, DocItem, Double> hyperbolicDecay() {
        return (seed, docItem) -> {
            ZonedDateTime zonedDateTime = DateUtils.getLegalDate(docItem.getPublishDate());
            long publishMillTime = Timestamp.from(zonedDateTime.toInstant()).getTime();
            long hourDiff = (seed - publishMillTime) / (1_000 * 3_600);
            double weekDiff = hourDiff * 1.0 / (24 * 7);
            double workerTimeDecay = workerTimeDecayRate();
            double decayWeight = workerTimeDecay / (weekDiff + 1) + (1 - workerTimeDecay);
            double oriScore = docItem.getOriScore();
            double finalScore = oriScore * decayWeight;
            log.debug("Use hyperbolic decay on [{}], got weight:[{}], original score:[{}] final score:[{}], original data:[{}]"
                    , handlerKey()
                    , decayWeight
                    , oriScore
                    , finalScore
                    , JSON.toJSONString(docItem)
            );
            return finalScore;
        };
    }

    // 统一乘以一个打折系数，适用于财报
    public BiFunction<Long, DocItem, Double> uniformDecay() {
        return (__, docItem) -> {
            double decayWeight = workerTimeDecayRate();
            double oriScore = docItem.getOriScore();
            double finalScore = oriScore * decayWeight;
            log.debug("Use uniform decay on [{}], got weight:[{}], original score:[{}] final score:[{}], original data:[{}]"
                    , handlerKey()
                    , decayWeight
                    , oriScore
                    , finalScore
                    , JSON.toJSONString(docItem)
            );
            return finalScore;
        };
    }

    private BiFunction<Long, DocItem, Double> defaultDecay() {
        return (__, docItem) -> {
            double decayWeight = confidence();
            double oriScore = docItem.getOriScore();
            double finalScore = oriScore * decayWeight;
            log.debug("Use default decay on [{}], got weight:[{}], original score:[{}] final score:[{}], original data:[{}]"
                    , handlerKey()
                    , decayWeight
                    , oriScore
                    , finalScore
                    , JSON.toJSONString(docItem)
            );
            return finalScore;
        };
    }

    protected Map<String, Object> doDataQuery(QueryRequest queryRequest, Map<String, Object> param) {
        return esClient().complexSearch(param);
    }

    @Override
    protected List<DocItem> doParseResult(QueryRequest queryRequest, Map<String, Object> queryResultMap) {
        List<DocItem> result = new ArrayList<>();
        List<Map<String, Object>> dataList = (List<Map<String, Object>>) queryResultMap.getOrDefault(ResultConfig.DATA_KEY, new ArrayList<>());
        if (CollectionUtils.isEmpty(dataList)) {
            log.warn("[{}] got no data", handlerKey());
            return result;
        }

        Set<String> distinct = ConcurrentHashMap.newKeySet();
        dataList.stream()
                .filter(map -> esDataFilter().apply(queryRequest, map))
                .map(esDataConverter())
                .filter(docItem -> StringUtils.isNotBlank(docItem.getFuncId()))
                .filter(docItem -> distinct.add(docItem.getFuncId()))
                .filter(docItem -> 0D < docItem.getDecayScore())
                .sorted(resultDataSorter())
                .forEach(result::add);

        log.info("[{}] built {} data", handlerKey(), result.size());
        return result;
    }

    private Comparator<DocItem> resultDataSorter() {
        return Comparator.comparing(DocItem::getFinalScore).reversed();
    }

    protected Function<Map<String, Object>, DocItem> esDataConverter() {
        return map -> {
            DocItem docItem = docItemBuilder(map);
            double decayScore = scoreDecay(docItem);
            docItem.setDecayScore(decayScore);
            double finalScore = decayScore * confidence();
            docItem.setFinalScore(finalScore);
            return docItem;
        };
    }

    protected DocItem docItemBuilder(Map<String, Object> map) {
        return DocItem.builder()
                .docType(dataType())
                .title((String) map.getOrDefault("title", ""))
                .summary((String) map.getOrDefault("summary", ""))
                .domain((String) map.getOrDefault("publisher", ""))
                .funcId((String) map.getOrDefault("bundleKey", ""))
                .bundleKey((String) map.getOrDefault("bundleKey", ""))
                .sourceUrl((String) map.getOrDefault("sourceUrl", ""))
                .publishDate((String) map.getOrDefault("publishDate", ""))
                .oriScore(((Float) map.getOrDefault(ESConfig.SCORE_KEY, defaultScore())).doubleValue())
                .build();
    }

    protected String dataType() {
        return SysConfigUtil.getAsString(operatorName(), "DATA_TYPE", "content");
    }

    protected abstract double defaultScore();

    protected BiFunction<QueryRequest, Map<String, Object>, Boolean> esDataFilter() {
        return (queryRequest, map) -> {
            if (CollectionUtils.isEmpty(map)) {
                return false;
            }

            double score = ((Float) map.getOrDefault(ESConfig.SCORE_KEY, defaultScore())).doubleValue();
            if (0D >= score) {
                return false;
            }

            String title = (String) map.getOrDefault("title", "");
            String bundleKey = (String) map.getOrDefault("bundleKey", "");
            String publishDate = (String) map.getOrDefault("publishDate", "");
            return StringUtils.isNoneBlank(title, bundleKey, publishDate);
        };
    }
}