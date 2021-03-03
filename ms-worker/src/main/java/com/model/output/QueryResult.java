package com.model.output;

import com.model.SortItem;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class QueryResult {
    private int total = 0;
    private List<String> highlight = new ArrayList<>();
    private List<SortItem> data = new ArrayList<>();
}
