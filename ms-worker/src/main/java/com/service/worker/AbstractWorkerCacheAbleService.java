package com.service.worker;

import com.model.input.QueryRequest;
import com.model.input.WorkerCoreQuery;
import com.service.CacheAbleCommonInterface;
import com.service.base.BaseCacheAbleService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Optional;

@Slf4j
@Service
public abstract class AbstractWorkerCacheAbleService<T, R> extends BaseCacheAbleService<T, R> implements CacheAbleCommonInterface<T, R> {

    public R defaultResult() { return (R) new ArrayList<>(); }

    public boolean invalidatedParam(T param) {
        if (super.invalidatedParam(param)) {
            return true;
        }

        WorkerCoreQuery coreQuery = new WorkerCoreQuery();
        if (param instanceof QueryRequest) {
            Optional<WorkerCoreQuery> opt = Optional.of(param).map(x -> (QueryRequest) x)
                    .flatMap(x -> Optional.ofNullable(x.getCoreQuery()));
            if (!opt.isPresent()) {
                return true;
            }

            coreQuery = opt.get();
        } else if (param instanceof WorkerCoreQuery) {
            coreQuery = (WorkerCoreQuery) param;
        }

        return ObjectUtils.isEmpty(coreQuery)
                || StringUtils.isBlank(coreQuery.getQuery())
                || CollectionUtils.isEmpty(coreQuery.getQuerySegments());
    }
}