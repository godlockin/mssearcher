package com.master.service.operators;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.common.SysConfigUtil;
import com.common.utils.ExtraCollectionUtils;
import com.model.SortItem;
import com.model.input.QueryRequest;
import com.service.base.BaseCacheAbleService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
public abstract class WorkerCallbackOperator extends BaseCacheAbleService<QueryRequest, List<SortItem>> {

    public List<SortItem> defaultResult() {
        return new ArrayList<>();
    }

    @Override
    protected Map<String, Object> doBuildParam(QueryRequest request) {
        return JSON.parseObject(JSON.toJSONString(request.getCoreQuery()));
    }

    @Override
    protected Map<String, Object> doDataQuery(QueryRequest request, Map<String, Object> param) {
        return doRemoteDataQuery(request, param);
    }

    protected boolean isLegalRemoteResult(Map<String, Object> remoteResultMap) {
        return super.isLegalRemoteResult(remoteResultMap) &&
                ExtraCollectionUtils.isNotEmpty((List<Map<String, Object>>) remoteResultMap.getOrDefault(resultDataKey(), new ArrayList<>()));
    }

    protected Map<String, Object> parseRemoteResult(Map<String, Object> remoteResultMap) {
        return remoteResultMap;
    }

    @Override
    protected List<SortItem> doParseResult(QueryRequest request, Map<String, Object> queryResultMap) {
        JSONArray array = (JSONArray) queryResultMap.getOrDefault(resultDataKey(), new JSONArray());
        List<SortItem> list = JSONArray.parseArray(JSONArray.toJSONString(array), SortItem.class);
        return list.parallelStream()
                .peek(sortItem -> sortItem.setDataType(dataType()))
                .collect(Collectors.toList());
    }

    protected String dataType() {
        return SysConfigUtil.getAsString(operatorName(), "DATA_TYPE", "default");
    }

    @Override
    protected Function<String, List<SortItem>> redisCacheLoadConverter() {
        return string -> JSONArray.parseArray(string, SortItem.class);
    }

    @Override
    protected Function<List<SortItem>, String> redisCacheSaveConverter() {
        return JSONArray::toJSONString;
    }
}
