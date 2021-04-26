package com.common;

import com.common.constants.ResultEnum;
import com.exception.MsWorkerException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OperatorConfigCache {
    private OperatorConfigCache() {
        throw new MsWorkerException(ResultEnum.ILLEGAL_METHOD);
    }

    private static ConcurrentHashMap<String, Double> cache = new ConcurrentHashMap<>();

    public static void add(String operator, Double confidence) {
        cache.put(operator, confidence);
    }

    public static double get(String operator) {
        return cache.getOrDefault(operator, 1D);
    }

    public static Map<String, Double> loadAll() {
        return new HashMap<>(cache);
    }
}
