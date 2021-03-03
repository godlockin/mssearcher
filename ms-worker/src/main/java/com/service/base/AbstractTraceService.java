package com.service.base;

import com.alibaba.fastjson.JSON;
import com.common.SysConfigUtil;
import com.common.utils.DataUtils;
import com.common.utils.HostUtils;
import com.model.config.SysConfigBean;
import com.service.CommonInterface;
import com.service.TraceServiceInterface;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public abstract class AbstractTraceService implements TraceServiceInterface {

    @Value("${SERVICE_CONFIG_ENABLE:true}")
    private boolean CONFIG_ENABLE;

    @Resource
    private ApplicationContext context;

    private Map<String, CommonInterface> operators;

    public Map<String, Object> loadSysConfig(boolean defaultConfig) {
        Map<String, Object> config = new LinkedHashMap<>();

        if (CONFIG_ENABLE) {
            if (defaultConfig) {
                operators.values().forEach(x -> config.put(x.operatorName(), x.findConfig()));
            } else {
                config.putAll(parseConfigMap(SysConfigUtil.get()));
            }
            config.put("HostName", HostUtils.getHostName());
            config.put("IP", HostUtils.getIp());
        }

        return config;
    }

    private Map<String, Object> parseConfigMap(Map<String, Object> stringObjectMap) {
        Map<String, Map<String, Object>> config = new ConcurrentHashMap<>();

        stringObjectMap.entrySet().parallelStream()
                .forEach(e -> {
                    MutablePair<String, String> pair = DataUtils.splitKey(e.getKey());
                    config.putIfAbsent(pair.getKey(), new HashMap<>());
                    config.get(pair.getKey()).put(pair.getValue(), e.getValue());
                });

        return new HashMap<>(config);
    }

    public String updSysConfig(SysConfigBean configItem) {

        String msg = "";
        if (CONFIG_ENABLE) {
            msg = configItemCheck(configItem);
            if (StringUtils.isBlank(msg)) {
                SysConfigUtil.set(configItem.getKey(), configItem.getValue());
                msg = String.format("Set [%s] as:[%s] now", configItem.getKey(), configItem.getValue());
            }
        }
        return msg;
    }

    private String configItemCheck(SysConfigBean configItem) {

        String errMsg = "";
        if (Objects.isNull(configItem)) {
            errMsg = "Can't find config item";
            log.error(errMsg);
            return errMsg;
        }

        if (StringUtils.isBlank(configItem.getKey()) || Objects.isNull(configItem.getValue())) {
            errMsg = "Can't find config key/value";
            log.error(errMsg);
            return errMsg;
        }

        String key = configItem.getKey();
        if (!SysConfigUtil.contains(key)) {
            errMsg = String.format("Key [%s] doesn't exists, pls check", key);
            log.error(errMsg);
            return errMsg;
        }

        return errMsg;
    }

    public String revertConfig(String operatorKey) {

        String msg;
        if (StringUtils.isBlank(operatorKey) || !operators.containsKey(operatorKey)) {
            msg = String.format("No operatorKey named:[%s] found", operatorKey);
            log.error(msg);
            return msg;
        }

        CommonInterface operator = operators.get(operatorKey).operatorRegister();

        msg = String.format("Operator:[%s] has reverted as default config:[%s]", operatorKey, JSON.toJSONString(operator.findConfig()));
        log.info(msg);

        return msg;
    }

    @PostConstruct
    void init() {
        operators = context.getBeansOfType(CommonInterface.class);
    }
}
