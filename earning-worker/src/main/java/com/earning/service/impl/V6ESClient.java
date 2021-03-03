package com.earning.service.impl;

import com.common.constants.Constants.ESConfig;
import com.common.constants.Constants.ResultConfig;
import com.common.datasource.ESClient;
import com.common.utils.ExtraCollectionUtils;
import com.model.config.ESConfigBean;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Data
@Slf4j
@Service
@SuppressWarnings({ "unchecked" })
public class V6ESClient extends ESClient {

    protected V6ESClient() {}

    public V6ESClient(ESConfigBean initConfig) {
        super(initConfig);
    }

    protected Map<String, Object> buildResult(SearchResponse response) {
        SearchHits hits = response.getHits();
        long total = hits.getTotalHits();
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

}
