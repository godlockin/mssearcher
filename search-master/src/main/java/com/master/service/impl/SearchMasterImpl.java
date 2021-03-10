package com.master.service.impl;

import com.common.SysConfigUtil;
import com.common.constants.Constants.ResultConfig;
import com.common.utils.DataUtils;
import com.master.service.SearchMaster;
import com.master.service.SearchService;
import com.model.SortItem;
import com.model.input.QueryRequest;
import com.model.input.WorkerCoreQuery;
import com.model.output.QueryResult;
import com.service.base.BaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class SearchMasterImpl extends BaseService<QueryRequest, QueryResult> implements SearchMaster {

    @Value("${SEARCH_INSTANCE_KEY:SEARCH_MASTER}")
    private String INSTANCE_KEY;
    @Value("${SEARCH_MASTER_HANDLER_KEY:SEARCH_MASTER}")
    private String HANDLER_KEY;
    @Value("${SEARCH_MASTER_OPERATOR_ACTIVE:true}")
    private boolean OPERATOR_ACTIVE;

    @Value("${SEARCH_MASTER_TIMEOUT:2000}")
    private int TIMEOUT;

    @Autowired
    private SearchService searchService;

    public QueryResult defaultResult() {
        return new QueryResult();
    }

    @Override
    protected Map<String, Object> doBuildParam(QueryRequest query) {
        return Collections.emptyMap();
    }

    @Override
    protected Map<String, Object> doDataQuery(QueryRequest queryRequest, Map<String, Object> param) {
        List<SortItem> docList = searchService.handle(queryRequest);
        WorkerCoreQuery coreQuery = queryRequest.getCoreQuery();
        List<SortItem> outList = DataUtils.handlePaging(coreQuery.getPageNo(), coreQuery.getPageSize(), docList);
        Map<String, Object> data = new HashMap<>();
        data.put(ResultConfig.TOTAL_KEY, docList.size());
        data.put(ResultConfig.DATA_KEY, outList);
        return data;
    }

    @Override
    protected QueryResult doParseResult(QueryRequest queryRequest, Map<String, Object> queryResultMap) {
        WorkerCoreQuery coreQuery = queryRequest.getCoreQuery();
        int total = (Integer) queryResultMap.get(ResultConfig.TOTAL_KEY);
        List<SortItem> docList = (List<SortItem>) queryResultMap.get(ResultConfig.DATA_KEY);
        List<String> highlight = coreQuery.getQuerySegments();
        return QueryResult.builder()
                .highlight(highlight)
                .data(docList)
                .total(total)
                .build();
    }

    @PostConstruct
    protected void init() {
        super.init();
        SysConfigUtil.set("ServiceInstance", "INSTANCE_KEY", INSTANCE_KEY);
    }

    @Override
    public Map<String, Object> findConfig() {
        Map<String, Object> config = super.findConfig();
        config.put("OPERATOR_ACTIVE", OPERATOR_ACTIVE);
        config.put("HANDLER_KEY", HANDLER_KEY);
        config.put("TIMEOUT", TIMEOUT);
        return config;
    }
}
