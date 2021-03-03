package com.common.constants;

public enum CacheTypeEnum {
    NONE
    , LOCAL
    , REDIS
    ;

    public static CacheTypeEnum parse(String cacheType) {
        for (CacheTypeEnum value : CacheTypeEnum.values()) {
            if (value.name().equalsIgnoreCase(cacheType)) {
                return value;
            }
        }
        return NONE;
    }
}
