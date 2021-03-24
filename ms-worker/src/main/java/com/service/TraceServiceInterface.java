package com.service;

import com.model.config.SysConfigBean;

import java.util.Map;

/**
 * Api to trace system statements and configs
 */
public interface TraceServiceInterface {

    /**
     * display the config of system
     *
     * @param defaultConfig
     *  - true for default config values
     *  - false for real config values in cache
     * @return config values
     */
    Map<String, Object> loadSysConfig(boolean defaultConfig);

    /**
     * modify the config value in current system
     *
     * @param configItem config key/value pair
     * @return whether config value has been changed
     */
    String updSysConfig(SysConfigBean configItem);

    /**
     * the real config value in current system
     *
     * @param operatorKey key for config
     * @return config value
     */
    String revertConfig(String operatorKey);
}
