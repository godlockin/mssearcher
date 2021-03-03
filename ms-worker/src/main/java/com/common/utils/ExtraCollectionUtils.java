package com.common.utils;

import org.apache.commons.lang3.ObjectUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

public class ExtraCollectionUtils extends CollectionUtils {

    public static boolean isNotEmpty(@Nullable Collection<?> collection) {
        return !isEmpty(collection);
    }

    public static boolean isNotEmpty(@Nullable Map<?, ?> map) {
        return !isEmpty(map);
    }

    public static boolean isAllEmpty(@Nullable Collection<?>... collections) {
        return ObjectUtils.isEmpty(collections) || Arrays.stream(collections).allMatch(CollectionUtils::isEmpty);
    }

    public static boolean isAnyEmpty(@Nullable Collection<?>... collections) {
        return ObjectUtils.isEmpty(collections) || Arrays.stream(collections).anyMatch(CollectionUtils::isEmpty);
    }

    public static boolean isNoneEmpty(@Nullable Collection<?>... collections) {
        return !isAnyEmpty(collections);
    }

    public static boolean isAllEmpty(@Nullable Map<?, ?>... maps) {
        return ObjectUtils.isEmpty(maps) || Arrays.stream(maps).allMatch(CollectionUtils::isEmpty);
    }

    public static boolean isAnyEmpty(@Nullable Map<?, ?>... maps) {
        return ObjectUtils.isEmpty(maps) || Arrays.stream(maps).anyMatch(CollectionUtils::isEmpty);
    }

    public static boolean isNoneEmpty(@Nullable Map<?, ?>... maps) {
        return !isAnyEmpty(maps);
    }
}
