package com.service;

import com.model.config.SysConfigBean;

import java.util.Map;

public interface TraceServiceInterface {
    Map<String, Object> loadSysConfig(boolean defaultConfig);

    String updSysConfig(SysConfigBean configItem);

    String revertConfig(String operatorKey);
}
