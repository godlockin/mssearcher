package com.model.input;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.beans.BeanUtils;

import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WorkerCoreQuery extends CoreQuery {

    protected int groupSize = 10;
    protected List<String> dataSources = new ArrayList<>();
    protected List<String> querySegments = new ArrayList<>();
    protected List<StockInfo> stockList = new ArrayList<>();
    protected List<ProjectInfo> projectList = new ArrayList<>();

    public WorkerCoreQuery(CoreQuery query) {
        BeanUtils.copyProperties(query, this);
    }
}
