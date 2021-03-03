package com.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SortItem {

    private double score;
    private String dataType;
    private boolean probablyMatch;

    private String title;
    private String bundleKey;
    private List<DocItem> dataList;
}
