package com.service.base;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.common.utils.ExtraCollectionUtils;
import com.common.utils.RestHttpClient;
import com.config.GlobalTasksPool;
import com.service.CommonInterface;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

@Slf4j
@Service
public abstract class BaseService<T, R> implements CommonInterface<T, R> {

    @Override
    public R handle(T t) { return fetchFunc(t); }

    protected R fetchFunc(T t) {
        R r = defaultResult();
        if (invalidatedParam(t)) {
            log.error("Invalidated param:[{}]", t);
            return r;
        }

        log.debug("{} do fetch", handlerKey());
        long start = System.nanoTime();
        Executor executor = GlobalTasksPool.getExecutor();
        CompletableFuture<R> task = CompletableFuture.supplyAsync(() -> doBuildParam(t), executor)
                .thenApplyAsync(map -> doDataQuery(t, map), executor)
                .thenApplyAsync(map -> doParseResult(t, map), executor)
                .whenCompleteAsync(completeLogBiConsumer(t, start), executor);

        int timeout = timeout();
        try {
            if (0 < timeout) {
                r = task.get(timeout, TimeUnit.MILLISECONDS);
            } else {
                r = task.get();
            }
        } catch (TimeoutException e) {
            log.error("[{}] took over [{}] mills to do query", operatorName(), timeout);
        } catch (Exception e) {
            log.error("[{}] got error to do query, with error:[{}]", operatorName(), e);
        }

        log.debug("{} done fetch", handlerKey());
        return r;
    }

    protected abstract Map<String, Object> doBuildParam(T t);
    protected abstract Map<String, Object> doDataQuery(T t, Map<String, Object> param);
    protected abstract R doParseResult(T t, Map<String, Object> queryResultMap);

    protected BiConsumer<R, Throwable> completeLogBiConsumer(T t, long start) {
        return (__, e) -> {
            long end = System.nanoTime();
            String msg = String.format("[%s] took [%s] to find result from param:[%s]"
                    , operatorName()
                    , (end - start) / 1_000_000
                    , t
            );

            if (Objects.nonNull(e)) {
                e.printStackTrace();
                String errMsg = String.format("%s, with error:[%s]"
                        , msg
                        , e.getLocalizedMessage()
                );

                log.error(errMsg);
            } else {
                log.info(msg);
            }
        };
    }

    protected Map<String, Object> doRemoteDataQuery(T t, Map<String, Object> param) {
        Map<String, Object> result = new HashMap<>();
        try {
            String remoteResult = RestHttpClient.doPost(remoteUrl(), param);
            Map<String, Object> remoteResultMap = new HashMap<>();
            if (StringUtils.isBlank(remoteResult)) {
                log.error("No result got from [{}]-[{}]", handlerKey(), remoteUrl());
            } else {
                remoteResultMap = JSONObject.parseObject(remoteResult);
            }

            if (isLegalRemoteResult(remoteResultMap)) {
                result.putAll(parseRemoteResult(remoteResultMap));
            } else {
                log.error("No ss result got from [{}]-[{}], got result as:[{}]", handlerKey(), remoteUrl(), remoteResult);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Error happened during operator:[{}] handle param:[{}] with error:[{}]"
                    , operatorName()
                    , JSON.toJSONString(param)
                    , e
            );
        }
        return result;
    }

    protected boolean isLegalRemoteResult(Map<String, Object> remoteResultMap) {
        String ssCode = ssCode();
        boolean isNotEmpty = ExtraCollectionUtils.isNotEmpty(remoteResultMap);
        if (StringUtils.isBlank(ssCode)) {
            return isNotEmpty;
        }

        return isNotEmpty && ssCode.equalsIgnoreCase(String.valueOf(remoteResultMap.getOrDefault(ssCodeKey(), "")).trim());
    }

    protected Map<String, Object> parseRemoteResult(Map<String, Object> remoteResultMap) {
        return StringUtils.isBlank(resultDataKey()) ? remoteResultMap :
                (Map<String, Object>) remoteResultMap.getOrDefault(resultDataKey(), new HashMap<>());
    }

    protected void init() {
        operatorRegister();
    }
}
