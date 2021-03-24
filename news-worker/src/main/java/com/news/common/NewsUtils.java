package com.news.common;

import com.common.constants.Constants;
import com.model.DocItem;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Map;

@Component
public class NewsUtils {

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
            String publishDate = (String) map.getOrDefault("publishTime", "");
            return StringUtils.isNoneBlank(title, bundleKey, publishDate);
    }

    public static DocItem docItemBuilder(String dataType, double defaultScore, Map<String, Object> map) {
        return DocItem.builder()
                .docType(dataType)
                .title((String) map.getOrDefault("headline", ""))
                .summary((String) map.getOrDefault("summary", ""))
                .funcId(map.getOrDefault("id", "").toString())
                .bundleKey((String) map.getOrDefault("uuid", ""))
                .oriScore(((Float) map.getOrDefault(Constants.ESConfig.SCORE_KEY, defaultScore)).doubleValue())
                .build();
    }
}
