package com.common;

import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class OperatorConfigCache {
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
