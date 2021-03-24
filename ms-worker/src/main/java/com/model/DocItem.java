package com.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DocItem {

    /**
     * data item type, e.g. doc, title, subtitle
     */
    private String docType;

    /**
     * original score calculated by ES
     */
    private Double oriScore;

    /**
     * decay score calculated by original score & decay weight
     */
    private Double decayScore;

    /**
     * final score calculated by decay score & data source/data type confidence
     */
    private Double finalScore;

    /**
     * doc's title
     */
    private String title;

    /**
     * doc's bundleKey
     */
    private String bundleKey;

    /**
     * doc snippet's id, e.g. image id in research
     */
    private String funcId;

    /**
     * doc snippet's title, e.g. sub title in research
     */
    private String funcTitle;

    /**
     * original source url
     */
    private String sourceUrl;

    /**
     * oss url
     */
    private String ossUrl;

    /**
     * image url
     */
    private String headImageUrl;

    /**
     * doc's publisher
     */
    private String domain;

    /**
     * doc's publish time
     */
    private String publishDate;

    /**
     * doc's summary
     */
    private String summary;

    /**
     * original doc's content, will not return to front end
     */
    private String content;

}
