package com.news.common;

import com.common.constants.Constants;
import com.common.constants.ResultEnum;
import com.common.utils.DateUtils;
import com.exception.MsWorkerException;
import com.model.DocItem;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Map;

public class NewsUtils {
    private NewsUtils() {
        throw new MsWorkerException(ResultEnum.ILLEGAL_METHOD);
    }

    public static boolean esDataJudgement(double defaultScore, Map<String, Object> map) {
        if (CollectionUtils.isEmpty(map)) {
            return false;
        }

        double score = ((Float) map.getOrDefault(Constants.ESConfig.SCORE_KEY, defaultScore)).doubleValue();
        if (0D >= score) {
            return false;
        }

        String title = (String) map.getOrDefault("headline", "");
        String bundleKey = (String) map.getOrDefault("uuid", "");
        Long publishTimestamp = (Long) map.getOrDefault("publishTimestamp", -1L);
        return StringUtils.isNoneBlank(title, bundleKey) && 0L < publishTimestamp;
    }

    public static DocItem docItemBuilder(String dataType, double defaultScore, Map<String, Object> map) {
        return DocItem.builder()
                .docType(dataType)
                .title((String) map.getOrDefault("headline", ""))
                .funcId(map.getOrDefault("id", "").toString())
                .bundleKey((String) map.getOrDefault("uuid", ""))
                .publishDate(DateUtils.getFormattedZonedDateTimeFromTimestamp((Long) map.get("publishTimestamp")))
                .oriScore(((Float) map.getOrDefault(Constants.ESConfig.SCORE_KEY, defaultScore)).doubleValue())
                .build();
    }

}
