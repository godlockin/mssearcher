package com.common.constants;

public enum DecayTypeEnum {
    NONE
    , SIGMOID
    , HYPERBOLIC
    , UNIFORM
    ;

    public static DecayTypeEnum parse(String cacheType) {
        for (DecayTypeEnum value : DecayTypeEnum.values()) {
            if (value.name().equalsIgnoreCase(cacheType)) {
                return value;
            }
        }
        return NONE;
    }
}
