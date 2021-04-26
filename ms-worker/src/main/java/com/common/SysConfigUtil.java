package com.common;


import com.common.constants.ResultEnum;
import com.common.utils.DataUtils;
import com.common.utils.ExtraCollectionUtils;
import com.exception.MsWorkerException;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class SysConfigUtil {
    private SysConfigUtil() {
        throw new MsWorkerException(ResultEnum.ILLEGAL_METHOD);
    }

    private static ConcurrentHashMap<String, Object> config = new ConcurrentHashMap<>();

    public static void set(String groupName, String key, Object value) {
        set(DataUtils.buildKey(groupName, key), value);
    }

    public static void set(String key, Object value) {
        config.put(key, value);
    }

    public static void set(String groupName, Map<String, Object> configs) {

        assert ExtraCollectionUtils.isNotEmpty(configs) : "Empty configs";

        Optional<Map.Entry<String, Object>> optionalEntry = configs.entrySet()
                .stream()
                .filter(e -> StringUtils.isBlank(e.getKey()) || ObjectUtils.isEmpty(e.getValue()))
                .findAny();
        assert !optionalEntry.isPresent() : "Empty key or value";

        Map<String, Object> tmp = new HashMap<>(configs);

        tmp.entrySet().parallelStream()
                .forEach(e -> config.put(DataUtils.buildKey(groupName, e.getKey()), e.getValue()));
    }

    public static boolean contains(String key) {
        return config.containsKey(key);
    }

    public static Map<String, Object> get() {
        return new HashMap<>(config);
    }

    public static Object get(String key, Object defaultValue, Class<?> clazz) {
        assert StringUtils.isNotBlank(key) : "Empty key";
        return clazz.cast(config.getOrDefault(key, defaultValue));
    }

    public static Boolean getAsBoolean(String groupName, String key, Boolean defaultValue) {
        return getAsBoolean(DataUtils.buildKey(groupName, key), defaultValue);
    }

    public static Boolean getAsBoolean(String key, Boolean defaultValue) {
        return (Boolean) get(key, defaultValue, Boolean.class);
    }

    public static String getAsString(String groupName, String key, String defaultValue) {
        return getAsString(DataUtils.buildKey(groupName, key), defaultValue);
    }

    public static String getAsString(String key, String defaultValue) {
        return (String) get(key, defaultValue, String.class);
    }

    public static Integer getAsInteger(String groupName, String key, Integer defaultValue) {
        return getAsInteger(DataUtils.buildKey(groupName, key), defaultValue);
    }

    public static Integer getAsInteger(String key, Integer defaultValue) {
        return (Integer) get(key, defaultValue, Integer.class);
    }

    public static Long getAsLong(String groupName, String key, Long defaultValue) {
        return getAsLong(DataUtils.buildKey(groupName, key), defaultValue);
    }

    public static Long getAsLong(String key, Long defaultValue) {
        return (Long) get(key, defaultValue, Long.class);
    }

    public static Double getAsDouble(String groupName, String key, Double defaultValue) {
        return getAsDouble(DataUtils.buildKey(groupName, key), defaultValue);
    }

    public static Double getAsDouble(String key, Double defaultValue) {
        return (Double) get(key, defaultValue, Double.class);
    }

    public static List<String> getAsStringList(String groupName, String key, List<String> defaultValue) {
        return getAsStringList(DataUtils.buildKey(groupName, key), defaultValue);
    }

    public static List<String> getAsStringList(String key, List<String> defaultValue) {
        return (List<String>) get(key, defaultValue, List.class);
    }
}