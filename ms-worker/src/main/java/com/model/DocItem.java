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
    private String docType;
    private Double oriScore;
    private Double decayScore;
    private Double finalScore;

    private String title;
    private String bundleKey;
    private String funcId;
    private String funcTitle;
    private String sourceUrl;
    private String ossUrl;
    private String domain;
    private String publishDate;

    private String summary;
    private String content;

}
